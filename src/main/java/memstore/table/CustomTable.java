package memstore.table;

import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.nio.IntBuffer;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Custom table implementation to adapt to provided query mix.
 */
public class CustomTable implements Table {
    static final short MAX_FIELD = 1024; // 0x400
    static final int BUCKET_BUFFER = 1000;
    static final int SHORT_FIELD_LEN = 2;
    static final int INT_FIELD_LEN = 4;

    private int worker_count;
    static private Worker[] workers;
    ArrayBlockingQueue<ResultMessage> result_queue;

    private int numCols;
    private int numRows;
    

    public CustomTable() { 

        // System.out.println("Creating Custom Table");
        // get the runtime object associated with the current Java application
        Runtime runtime = Runtime.getRuntime();
        
        // get the number of processors available to the Java virtual machine
        int numberOfProcessors = runtime.availableProcessors();
        System.out.println("numberOfProcessors: " + numberOfProcessors);

        // We are using double the number of Logical thread. 
        int i = 0;
        for (i = 0; i < 5; i++) {
            numberOfProcessors = numberOfProcessors / 2;
            if (numberOfProcessors == 0) {
                break;
            }
        }
        worker_count = (1 << (i));
        //worker_count = 8;
          
        System.out.println("Number of workers: " + worker_count);
    }

    public class OpMessage {
        /**
         *  0: Exit,
         *  1: Get(var1 : row, var2 : col),
         *  2: Put(var1 : row, var2 : col, var3 : field, queue : transmition),
         *  3: GetColumnSum(),
         *  4: GetPredicatedColumnSum(var1 : threshold1, var2 : threshold2),
         *  5: GetPredicatedAllColumnsSum(var1 : threshold),
         *  6: GetPredicatedUpdate(var1 : threshold),
         */
        byte op;
        short var3;
        int var1, var2;
        /**
         * Buffer will be in the following format.
         * <p>
         * [col4] ... [coln] [col1] [col2] [col3]
         * <p>
         * other than col3, all column will be short, col will be int.
         */
        ArrayBlockingQueue<ByteBuffer> queue;
    }

    public class ResultMessage {
        long var;
    }

    public class Worker extends Thread {
        int queue_size = 200;
        public ArrayBlockingQueue<OpMessage> command_queue;
        public ArrayBlockingQueue<ResultMessage> result_queue;

        int num_of_buckets;
        int mask, mask_p1, index;
        int total_cols;

        ShortBucket buckets1[];
        IntBucket buckets2[];
        IntBucket buckets3[];
        LargeShortBucket bucket_rest[];

        long col0_sum;
        long bucket_sum_all[]; // This sum includes all cols
        long bucket_sum_col2[]; // This sum includes only col2
        

        // col0_index is in the following format
        // [row0_col0, row0_index] [row0_col0, row0_index]
        IntBuffer col0_index;
        IntBucket bucket_row[]; // This is the reverse mapping of col0_index

        public class IntBucket {
            public int size, capacity;
            public IntBuffer rows;

            public IntBucket(int capacity) {
                this.size = 0;
                this.capacity = capacity;
                this.rows = IntBuffer.allocate(capacity);
            }

            public void pushInt(int field) {
                this.rows.put(this.size, field);
                this.size++;
            }

            public int getInt(int rowId) {
                return this.rows.get(rowId);
            }
        }

        public class ShortBucket {
            public int size, capacity;
            public ShortBuffer rows;

            public ShortBucket(int capacity) {
                this.size = 0;
                this.capacity = capacity;
                this.rows = ShortBuffer.allocate(capacity);
            }

            public void pushShort(short field) {
                this.rows.put(this.size, field);
                this.size++;
            }

            public short getShort(int rowId) {
                return this.rows.get(rowId);
            }
        }

        public class LargeShortBucket {
            public int size, capacity;
            public int num_cols;
            public ShortBuffer rows;

            public LargeShortBucket(int capacity, int num_cols) {
                this.size = 0;
                this.capacity = capacity;
                this.num_cols = num_cols;
                this.rows = ShortBuffer.allocate(num_cols * capacity);
            }

            public void pushShort(int col, short field) {
                this.rows.put(num_cols * this.size + col, field);
            }

            public void next() {
                this.size++;
            }

            public int getShort(int rowId, int colId) {
                return this.rows.get(num_cols * rowId + colId);
            }

            public void putShort(int rowId, int colId, short field) {
                this.rows.put(num_cols * rowId + colId, field);
            }
        }

        /**
         * In the custom table, we store col1, col2, col3 in each bucket of col0 [0, 1024)
         * We also stores a index on RowId to Col0_bucket + bucket_index (So we could perform the update)
         * For the rest of the colunums, we will store them using row format. 
         */
        public Worker(DataLoader loader, int mask, int index, ArrayBlockingQueue<ResultMessage> result_queue) {
            try {
            // if mask is 0xf and index = 0x2
            // This means for a record that col0 = 0x2e, it's not in the current worker
            // And for a record that col0 = ox22, it's in the bucket with bucket num 0x2
            System.out.println("Starting worker!!!!");
            this.mask = mask;
            this.mask_p1 = mask + 1;
            this.index = index;
            this.num_of_buckets = CustomTable.MAX_FIELD / this.mask_p1;
            this.command_queue = new ArrayBlockingQueue<OpMessage>(queue_size);
            this.result_queue = result_queue;
            this.col0_sum = 0;

            int numCols = loader.getNumCols();
            List<ByteBuffer> rows = loader.getRows();
            int total_rows = rows.size();
            this.total_cols = loader.getNumCols();
            int numRow[] = new int[num_of_buckets];
            for (int i = 0; i < num_of_buckets; i++) {
                numRow[i] = 0;
            }

            // Calculating the col0_sum
            for (int i = 0; i < numRows; i++) {
                int field0 = rows.get(i).getInt(0);
                if ((field0 & this.mask) != this.index) continue;
                this.col0_sum = this.col0_sum + rows.get(i).getInt(0);
            }

            // calculating the number of row's in each bucket
            for (int rowId = 0; rowId < total_rows; rowId++) {
                ByteBuffer curRow = rows.get(rowId);
                int field0 = curRow.getInt(0);
                if (field0 < 0 || field0 >= CustomTable.MAX_FIELD) {
                    throw new IllegalArgumentException("Get field out of range: " + field0);
                }
                int field_index = field0 & this.mask;
                if (field_index == index) {
                    numRow[field0 / this.mask_p1]++;
                }
            }

            // Fill in the bucket(col1, col2, col3) for each row as well as the index
            this.buckets1 = new ShortBucket[num_of_buckets];
            this.buckets2 = new IntBucket[num_of_buckets];
            this.buckets3 = new IntBucket[num_of_buckets];
            this.bucket_row = new IntBucket[num_of_buckets];
            this.bucket_rest = new LargeShortBucket[num_of_buckets];
            this.bucket_sum_all = new long[num_of_buckets];
            this.bucket_sum_col2 = new long[num_of_buckets];
            this.col0_index = IntBuffer.allocate(2 * total_rows);
            for (int i = 0; i < 2 * total_rows; i++) {
                this.col0_index.put(i, -1);
            }
            
            for (int i = 0; i < num_of_buckets; i++) {
                bucket_row[i] = new IntBucket(numRow[i] + CustomTable.BUCKET_BUFFER);
                buckets1[i] = new ShortBucket(numRow[i] + CustomTable.BUCKET_BUFFER);
                buckets2[i] = new IntBucket(numRow[i] + CustomTable.BUCKET_BUFFER);
                buckets3[i] = new IntBucket(numRow[i] + CustomTable.BUCKET_BUFFER);
                bucket_rest[i] = new LargeShortBucket(numRow[i] + CustomTable.BUCKET_BUFFER, numCols - 4);
                this.bucket_sum_all[i] = 0L;
            }

            for (int rowId = 0; rowId < total_rows; rowId++) {
                ByteBuffer curRow = rows.get(rowId);
                int field0 = curRow.getInt(0);
                if ((field0 & this.mask) != this.index) continue;
                int field1 = curRow.getInt(CustomTable.INT_FIELD_LEN);
                int field2 = curRow.getInt(CustomTable.INT_FIELD_LEN * 2);
                int field3 = curRow.getInt(CustomTable.INT_FIELD_LEN * 3);
                int bucket_index = field0 / this.mask_p1;
                // Push col1, col2, col3 into buckets
                bucket_row[bucket_index].pushInt(rowId);
                buckets1[bucket_index].pushShort((short)field1);
                buckets2[bucket_index].pushInt(field2);
                buckets3[bucket_index].pushInt(field3);
                // Adding col0, col1, col2 to the bucket sum. (col3 is left out intentionally)
                this.bucket_sum_all[bucket_index] = this.bucket_sum_all[bucket_index] + field0;
                this.bucket_sum_all[bucket_index] = this.bucket_sum_all[bucket_index] + field1;
                this.bucket_sum_all[bucket_index] = this.bucket_sum_all[bucket_index] + field2;
                this.bucket_sum_all[bucket_index] = this.bucket_sum_all[bucket_index] + field3;

                this.bucket_sum_col2[bucket_index] = this.bucket_sum_col2[bucket_index] + field2;

                // Calculate col0 index (rowId -> col0 position)
                this.col0_index.put(2 * rowId, bucket_index);
                this.col0_index.put(2 * rowId + 1, this.buckets1[bucket_index].size - 1);
                // push other columns to large_bucket
                for (int j = 4; j < numCols; j++) {
                    int field = curRow.getInt(CustomTable.INT_FIELD_LEN * j);
                    this.bucket_rest[bucket_index].pushShort(j - 4, (short)field);
                    // Add the field to the bucket-wise sum.
                    this.bucket_sum_all[bucket_index] = this.bucket_sum_all[bucket_index] + field;
                }
                this.bucket_rest[bucket_index].next();
            }
            }
            catch (Exception e) {
                System.out.println(e);
                System.exit(200 + this.index);
            }
        }
        
        @Override
        public void run() {
            System.out.println("MyThread running");
            try {

                while (true) {
                    verify();
                    OpMessage command = command_queue.poll(20, TimeUnit.SECONDS);
                    if (command == null || command.op == 0) {
                        break;
                    } else if (command.op == 1) {
                        // System.out.println("Command 1");
                        // GetIntField
                        int field = workerGetIntField(command.var1, command.var2);
                        if (field < 0) continue;
                        // System.out.println("---Got field: " + field);
                        // System.out.println("Got field row: " + command.var1);
                        // System.out.println("Got field col: " + command.var2);
                        // System.out.println("Got field worker index: " + this.index);
                        ResultMessage result = new ResultMessage();
                        result.var = (long)field;
                        result_queue.add(result);
                    } else if (command.op == 2) {
                        // System.out.println("Command 2");
                        workerPutIntField(command.var1, command.var2, command.var3, command.queue);
                    } else if (command.op == 3) {
                        // System.out.println("Command 3");
                        long sum = this.col0_sum;
                        ResultMessage result = new ResultMessage();
                        result.var = sum;
                        result_queue.add(result);
                    } else if (command.op == 4) { 
                        // System.out.println("Command 4");
                        long sum = workerPredicatedColumnSum(command.var1, command.var2);
                        ResultMessage result = new ResultMessage();
                        result.var = sum;
                        result_queue.add(result);
                    } else if (command.op == 5) {
                        // System.out.println("Command 5");
                        long sum = workerPredicatedAllColumnsSum(command.var1);
                        ResultMessage result = new ResultMessage();
                        // System.out.println("---Got sum: " + sum);
                        result.var = sum;
                        result_queue.add(result);
                    } else if (command.op == 6) {
                        // System.out.println("Command 6");
                        int count = workerPredicatedUpdate(command.var1);
                        ResultMessage result = new ResultMessage();
                        result.var = (long)count;
                        //System.out.println("---Got count: " + count);
                        result_queue.add(result);
                    } else {
                        System.out.println("Command " + command.op);
                        //Thread.sleep(4000);
                    }
                }
            } catch (Exception e) {
                System.out.println(e);
                System.exit(100);
            }
            System.out.println("Worker stops: " + index);
        }

        private void panic(int code, String msg) {
            System.out.println("!!!!!!!!!PANIC:" + msg);
            System.exit(code);
        }

        public void verify() {
            // // System.out.println("Verifying!!!!!!!");
            
            // // for each bucket, they should have the same size
            // for (int bucket = 0; bucket < num_of_buckets; bucket++) {
            //     int row_size = bucket_row[bucket].size;
            //     if (buckets1[bucket].size != row_size) panic(101, "buckets1[" + bucket + "]size wrong, row: " + row_size + "bucket1: " + buckets1[bucket].size);
            //     if (buckets2[bucket].size != row_size) panic(102, "buckets2[" + bucket + "]size wrong, row: " + row_size + "bucket2: " + buckets2[bucket].size);
            //     if (buckets3[bucket].size != row_size) panic(103, "buckets3[" + bucket + "]size wrong, row: " + row_size + "bucket2: " + buckets3[bucket].size);
            //     if (bucket_rest[bucket].size != row_size) panic(104, "bucket_rest[" + bucket + "]size wrong, row: " + row_size + "bucket_rest: " + bucket_rest[bucket].size);
            // }
            // // for each rowId, we verify that the corresponding bucket/index map back
            // for (int rowId = 0; rowId < numRows; rowId++) {
            //     int bucket_id = this.col0_index.get(rowId * 2);
            //     int bucket_index = this.col0_index.get(rowId * 2 + 1);
            //     // First make sure they are within range
            //     if (bucket_id == -1) continue;
            //     if (bucket_index >= bucket_row[bucket_id].size) panic(105, "bucket index out of range. row: " + rowId + ",bucket_id:" + bucket_id + ",bucket_index:" + bucket_index);
            //     int calculated_row = this.bucket_row[bucket_id].getInt(bucket_index);
            //     if (calculated_row != rowId) panic(106, "row: " + rowId + ",calculated_row:" + calculated_row);
            // }
            // // for each rowId, we verify that the corresponding bucket/index map back
            // for (int bucket = 0; bucket < num_of_buckets; bucket++) {
            //     IntBucket row_bucket = this.bucket_row[bucket];
            //     for (int bucket_index = 0; bucket_index < row_bucket.size; bucket_index++) {
            //         int row_id = row_bucket.getInt(bucket_index);
            //         int mapped_bucket = this.col0_index.get(row_id * 2);
            //         int mapped_bucket_index = this.col0_index.get(row_id * 2 + 1);
            //         if(mapped_bucket != bucket) panic(107, "mapped_bucket: " + mapped_bucket + ",bucket:" + bucket);
            //         if(mapped_bucket_index != bucket_index) panic(108, "mapped_bucket_index: " + mapped_bucket_index + ",bucket_index:" + bucket_index);
            //     }
            // }
        }

        private int workerGetIntField(int rowId, int colId) {
            int bucket = this.col0_index.get(2 * rowId);
            if (bucket < 0) return -1;
            // System.out.println("getting number from worker: " + index);
            if (colId == 0) return bucket * this.mask_p1 + this.index;
            int bucket_index = this.col0_index.get(2 * rowId + 1);
            if (colId == 1) {
                return this.buckets1[bucket].getShort(bucket_index);
            } else if (colId == 2) {
                return this.buckets2[bucket].getInt(bucket_index);
            } else if (colId == 3) {
                return this.buckets3[bucket].getInt(bucket_index);
            } else {
                return this.bucket_rest[bucket].getShort(bucket_index, colId - 4);
            }
        }

        private void workerPutIntField(int rowId, int colId, int field, ArrayBlockingQueue<ByteBuffer> queue) throws Exception {
            int bucket = this.col0_index.get(2 * rowId);
            if (bucket < 0) {
                // The row doesn't belong to us
                // Check if we are the receiver
                if (colId == 0 && (field & this.mask) == index) {
                    // We are the receiver.
                    ByteBuffer row = queue.take();

                    bucket = field / this.mask_p1;
                    // System.out.println("receiving number, putting in : " + bucket);

                    // Update col0 index
                    this.col0_index.put(rowId * 2, bucket);
                    this.col0_index.put(rowId * 2 + 1, this.bucket_rest[bucket].size);

                    // Add the row to the right place
                    row.position(0);
                    row.asShortBuffer().get(this.bucket_rest[bucket].rows.array(), this.bucket_rest[bucket].size * (total_cols - 4), total_cols - 4);
                    this.bucket_rest[bucket].next();
                    this.bucket_row[bucket].pushInt(rowId);
                    this.buckets1[bucket].pushShort(row.getShort((total_cols - 4) * CustomTable.SHORT_FIELD_LEN));
                    int field2 = row.getShort((total_cols - 3) * CustomTable.SHORT_FIELD_LEN);
                    this.buckets2[bucket].pushInt(field2);
                    int field3 = row.getInt((total_cols - 2) * CustomTable.SHORT_FIELD_LEN);
                    this.buckets3[bucket].pushInt(field3);

                    // Update sum (both all number sum and col0 sum)
                    this.col0_sum = this.col0_sum + field;
                    this.bucket_sum_all[bucket] = this.bucket_sum_all[bucket] + field + field3;
                    for (int i = 0; i < total_cols - 2; i++) {
                        this.bucket_sum_all[bucket] = this.bucket_sum_all[bucket] + row.asShortBuffer().get(i);
                    }
                    this.bucket_sum_col2[bucket] = this.bucket_sum_col2[bucket] + field2;
                    return;
                } else {
                    // The row doesn't belongs to us and we are not the receiver
                    return;
                }
            }
            int bucket_index = this.col0_index.get(2 * rowId + 1);
            if (colId > 3) {
                // Updating non-indexed column
                // primary index (col0) not affected
                int old_value = this.bucket_rest[bucket].getShort(bucket_index, colId - 4);
                if (old_value == field)  {
                    // Nothing changed.
                    return;
                }
                this.bucket_rest[bucket].putShort(bucket_index, colId - 4, (short)field);
                this.bucket_sum_all[bucket] = this.bucket_sum_all[bucket] + field - old_value;
            } else if (colId == 0) {
                // We are updating our primary index now.
                if (bucket * this.mask_p1 + bucket_index == field) {
                    // Nothing changed.
                    return;
                }
                // First we need to decide if we still own the row anymore.
                int field_index = field & this.mask;
                int field_bucket = field / this.mask_p1;
                if (field_index == this.index) {
                    // We still own the row, it's just a matter of moving it around
                    int old_col0 = bucket * this.mask_p1 + index;
                    // We know that the col0_sum is only changed due to field change
                    this.col0_sum = this.col0_sum + field - old_col0;
                    int new_bucket_index = this.bucket_row[field_bucket].size;

                    // Add the row to the new bucket
                    // Old Value location: bucket - bucket_index
                    // New Value location: field_bucket - new_bucket_index
                    // Move old value to new location
                    short col1 = this.buckets1[bucket].getShort(bucket_index);
                    int col2 = this.buckets2[bucket].getInt(bucket_index);
                    int col3 = this.buckets3[bucket].getInt(bucket_index);

                    this.bucket_row[field_bucket].pushInt(rowId);
                    this.buckets1[field_bucket].pushShort(col1);
                    this.buckets2[field_bucket].pushInt(col2);
                    this.buckets3[field_bucket].pushInt(col3);

                    this.bucket_rest[field_bucket].rows.position(new_bucket_index * (total_cols - 4));
                    this.bucket_rest[field_bucket].rows.put(this.bucket_rest[bucket].rows.array(), bucket_index * (total_cols - 4), (total_cols - 4));
                    this.bucket_rest[field_bucket].next();

                    // Updatet the row to reflect the new position
                    this.col0_index.put(2 * rowId, field_bucket);
                    this.col0_index.put(2 * rowId + 1, new_bucket_index);
                    // System.out.println("------111-------------: " + field_bucket + ", " + new_bucket_index);
                    // System.out.println("total_cols:" + total_cols);
                    // System.out.println("bucket:" + bucket);
                    // System.out.println("new_bucket_index:" + new_bucket_index);
                    // System.out.println("bucket first row id:" + this.bucket_rest[bucket].getShort(0, 0));
                    // System.out.println("bucket first row id:" + this.bucket_rest[field_bucket].getShort(1, 0));
                    // Add it to sum
                    long sum = 0;
                    sum = sum + col1 + col2 + col3;
                    for (int i = 0; i < total_cols - 4; i++) {
                        sum = sum + this.bucket_rest[field_bucket].getShort(this.bucket_rest[field_bucket].size - 1, i);
                    }

                    this.bucket_sum_all[field_bucket] = this.bucket_sum_all[field_bucket] + sum + field;

                    // Then we need to delete the old value
                    // We delete the old value by moving the last row to replace it
                    if (bucket_index + 1 == this.bucket_rest[bucket].size) {
                        // System.out.println("------111--------We are moving rows---------: " + bucket_index + ", " + this.bucket_rest[bucket].size);
                        // We are deleting the last line, simply update the size and we are done
                        this.bucket_row[bucket].size--;
                        this.buckets1[bucket].size--;
                        this.buckets2[bucket].size--;
                        this.buckets3[bucket].size--;
                        this.bucket_rest[bucket].size--;
                        verify();
                    } else {
                        // receiving numbereceiving number, puttingSystem.out.println("--------------We are moving rows---------");
                        // We are not the last row, we will move the last row to replace the old row
                        this.bucket_row[bucket].size--;
                        this.buckets1[bucket].size--;
                        this.buckets2[bucket].size--;
                        this.buckets3[bucket].size--;
                        this.bucket_rest[bucket].size--;
                        int last_row = this.buckets1[bucket].size;

                        int moved_row = this.bucket_row[bucket].getInt(last_row);
                        this.bucket_row[bucket].rows.put(bucket_index, moved_row);
                        this.buckets1[bucket].rows.put(bucket_index, this.buckets1[bucket].getShort(last_row));
                        this.buckets2[bucket].rows.put(bucket_index, this.buckets2[bucket].getInt(last_row));
                        this.buckets3[bucket].rows.put(bucket_index, this.buckets3[bucket].getInt(last_row));
                        // We promise that the row read from and the row write to will not overlap
                        this.bucket_rest[bucket].rows.position(bucket_index * (total_cols - 4));
                        this.bucket_rest[bucket].rows.put(this.bucket_rest[bucket].rows.array(), last_row * (total_cols - 4), (total_cols - 4));

                        // Updatet the moved row to reflect the new position
                        this.col0_index.put(2 * moved_row, bucket);
                        this.col0_index.put(2 * moved_row + 1, bucket_index);
                    }
                    // remove the old value from bucket_sum
                    this.bucket_sum_all[bucket] = this.bucket_sum_all[bucket] - sum - old_col0;
                    this.bucket_sum_col2[bucket] = this.bucket_sum_col2[bucket] - col2;

                    // We also need to update the rowId -> bucket, bucket_index mapping


                } else {
                    // We need to transfer the row to another worker
                    // First, we copy the row to a ByteBuffer and send it out
                    // All except col3 (4 Bytes) takes 2 Bytes, so total is total_cols * CustomTable.SHORT_FIELD_LEN
                    ByteBuffer m = ByteBuffer.allocate(total_cols * CustomTable.SHORT_FIELD_LEN);
                    m.position(0);
                    m.asShortBuffer().put(this.bucket_rest[bucket].rows.array(), bucket_index * (total_cols - 4), (total_cols - 4));
                    short col1 = this.buckets1[bucket].getShort(bucket_index);
                    short col2 = (short)this.buckets2[bucket].getInt(bucket_index);
                    int col3 = this.buckets3[bucket].getInt(bucket_index);
                    m.putShort((total_cols - 4) * CustomTable.SHORT_FIELD_LEN, col1);
                    m.putShort((total_cols - 3) * CustomTable.SHORT_FIELD_LEN, col2);
                    m.putInt((total_cols - 2) * CustomTable.SHORT_FIELD_LEN, col3);

                    // Calculating the row sum before the row is replaced
                    int old_col0 = bucket * this.mask_p1 + index;
                    long sum = 0;
                    sum = sum + col1 + col2 + col3;
                    for (int i = 0; i < total_cols - 4; i++) {
                        sum = sum + m.asShortBuffer().get(i);
                    }
                    // Message out
                    queue.add(m);

                    // Delete the row's col0 index
                    this.col0_index.put(2 * rowId, -1);
                    this.col0_index.put(2 * rowId + 1, -1);

                    // Then we need to delete the old value
                    // We delete the old value by moving the last row to replace it
                    if (bucket_index + 1 == this.bucket_rest[bucket].size) {
                        // System.out.println("------111--------We are moving rows---------: " + bucket_index + ", " + this.bucket_rest[bucket].size);
                        // We are deleting the last line, simply update the size and we are done
                        this.bucket_row[bucket].size--;
                        this.buckets1[bucket].size--;
                        this.buckets2[bucket].size--;
                        this.buckets3[bucket].size--;
                        this.bucket_rest[bucket].size--;
                        verify();
                    } else {
                        // System.out.println("--------------We are moving rows---------");
                        // We are not the last row, we will move the last row to replace the old row
                        this.bucket_row[bucket].size--;
                        this.buckets1[bucket].size--;
                        this.buckets2[bucket].size--;
                        this.buckets3[bucket].size--;
                        this.bucket_rest[bucket].size--;
                        int last_row = this.buckets1[bucket].size;

                        int moved_row = this.bucket_row[bucket].getInt(last_row);
                        this.bucket_row[bucket].rows.put(bucket_index, moved_row);
                        this.buckets1[bucket].rows.put(bucket_index, this.buckets1[bucket].getShort(last_row));
                        this.buckets2[bucket].rows.put(bucket_index, this.buckets2[bucket].getInt(last_row));
                        this.buckets3[bucket].rows.put(bucket_index, this.buckets3[bucket].getInt(last_row));
                        // We promise that the row read from and the row write to will not overlap
                        this.bucket_rest[bucket].rows.position(bucket_index * (total_cols - 4));
                        this.bucket_rest[bucket].rows.put(this.bucket_rest[bucket].rows.array(), last_row * (total_cols - 4), (total_cols - 4));

                        // Updatet the moved row to reflect the new position
                        this.col0_index.put(2 * moved_row, bucket);
                        this.col0_index.put(2 * moved_row + 1, bucket_index);
                    }
                    // Delete it from sum
                    this.bucket_sum_all[bucket] = this.bucket_sum_all[bucket] - sum - old_col0;
                    this.bucket_sum_col2[bucket] = this.bucket_sum_col2[bucket] - col2;
                    // We don't own the row anymore, delete it from col0_sum
                    this.col0_sum = this.col0_sum - old_col0;

                }
            } else if (colId == 1) {
                int old_field = this.buckets1[bucket].rows.get(bucket_index);
                // Update the sum
                this.bucket_sum_all[bucket] = this.bucket_sum_all[bucket] - old_field + field;
                this.buckets1[bucket].rows.put(bucket_index, (short)field);
            } else if (colId == 2) {
                int old_field = this.buckets2[bucket].rows.get(bucket_index);
                // Update the sum
                this.bucket_sum_all[bucket] = this.bucket_sum_all[bucket] - old_field + field;
                this.bucket_sum_col2[bucket] = this.bucket_sum_col2[bucket] - old_field + field;
                this.buckets2[bucket].rows.put(bucket_index, field);
            } else if (colId == 3) {
                int old_field = this.buckets3[bucket].rows.get(bucket_index);
                // Update the sum
                this.buckets3[bucket].rows.put(bucket_index, field);
                this.bucket_sum_all[bucket] = this.bucket_sum_all[bucket] - old_field + field;
            } else {
                throw new IllegalArgumentException("Getting wrong colId in PutIntField: " + colId);
            }
        }

        /**
         * Implements the query
         *  SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
         *
         *  Returns the sum of all elements in the first column of the table,
         *  subject to the passed-in predicates.
         */
        public long columnSum() {
            return this.col0_sum;
        }

        /**
         * Implements the query
         *  SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
         *
         *  Returns the sum of all elements in the first column of the table,
         *  subject to the passed-in predicates.
         */
        public long workerPredicatedColumnSum(int threshold1, int threshold2) {
            long sum = 0;
            for(int bucket = 0; bucket < num_of_buckets; bucket++) {
                int bucket_size = this.buckets1[bucket].size;
                for (int j = 0; j < bucket_size; j++) {
                    if ((int)this.buckets1[bucket].getShort(j) > threshold1 && this.buckets2[bucket].getInt(j) < threshold2) {
                        sum = sum + bucket * this.mask_p1 + this.index;
                    }
                }
            }
            return sum;
        }

        /**
         * Implements the query
         *   UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
         *
         *   Returns the number of rows updated.
         */
        private int workerPredicatedUpdate(int threshold) {
            if (threshold < 0) return 0;
            int count = 0;
            int treshold_bucket = threshold / this.mask_p1;
            int threshold_index = threshold & this.mask;
            int end_bucket;
            if (this.index < threshold_index) {
                end_bucket = treshold_bucket;
            } else {
                end_bucket = treshold_bucket - 1;
            }

            for (int i = 0; i <= end_bucket; i++) {
                count = count + this.buckets3[i].size;
                this.bucket_sum_all[i] = this.bucket_sum_all[i] + this.bucket_sum_col2[i];
                for (int j = 0; j < this.buckets3[i].size; j++) {
                    this.buckets3[i].rows.array()[j] = this.buckets3[i].rows.array()[j] + this.buckets2[i].rows.array()[j];
                }
            }



            return count;
        }

        /**
         * Implements the query
         *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
         *
         *  Returns the sum of all elements in the rows which pass the predicate.
         */
        public long workerPredicatedAllColumnsSum(int threshold) {
            // We keep the sum of all bucket except col3.
            long sum = 0;
            int start_bucket;
            if (threshold >= 0) {
                int treshold_bucket = threshold / this.mask_p1;
                int threshold_index = threshold & this.mask;
                if (this.index > threshold_index) {
                    start_bucket = treshold_bucket;
                } else {
                    start_bucket = treshold_bucket + 1;
                }
            } else {
                start_bucket = 0;
            }

            // @TODO: optimize this part (split into multiple add)
            for (int i = start_bucket; i < this.num_of_buckets; i++) {
                sum = sum + this.bucket_sum_all[i];
            }
            return sum;

        }
    }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException {
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();
        System.out.println("total rows: " + numRows);

        result_queue = new ArrayBlockingQueue<ResultMessage>(1000);
        int mask = this.worker_count - 1;
        try {
            if (workers != null) {
                for (Worker worker : workers) {
                    if (worker == null) continue;
                    OpMessage m = new OpMessage();
                    m.op = 0;
                    worker.command_queue.add(m);
                    worker.join();
                }
            }
        } catch (Exception e) {
            System.out.println("CustomTable.load()" + e);
            System.exit(100);
        }

        try {
            workers = new Worker[worker_count];
            for (int j = 0; j < worker_count; j++) {
                // System.out.println("mask: " + mask + ",index: " + j);
                workers[j] = new Worker(loader, mask, j, result_queue);
                workers[j].start();
            }
        } catch (Exception e) {
            System.out.println(e);
            System.exit(100);
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        for (Worker worker : workers) {
            OpMessage m = new OpMessage();
            m.op = 1;
            m.var1 = rowId;
            m.var2 = colId;
            worker.command_queue.add(m);
        }
        try {
            ResultMessage result_message = result_queue.poll(4, TimeUnit.SECONDS);
            if (result_message == null) {
                System.out.println("Time out while wait for GetIntField");
                System.exit(100);
            }
            return (int)result_message.var;
        } catch (Exception e) {
            System.out.println("GetIntField Failed. This should not happen: " + e);
            System.exit(100);
            return -1;
        }
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        ArrayBlockingQueue<ByteBuffer> queue = new ArrayBlockingQueue<ByteBuffer>(1);
        for (Worker worker : workers) {
            OpMessage m = new OpMessage();
            m.op = 2;
            m.var1 = rowId;
            m.var2 = colId;
            m.var3 = (short)field;
            m.queue = queue;
            worker.command_queue.add(m);
        }
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table;
     *
     *  Returns the sum of all elements in the first column of the table.
     */
    @Override
    public long columnSum() {
        long sum = 0;
        
        for (Worker worker : workers) {
            OpMessage m = new OpMessage();
            m.op = 3;
            worker.command_queue.add(m);
        }
        try {
            for (int i = 0; i < workers.length; i++) {
                ResultMessage result_message = result_queue.poll(4, TimeUnit.SECONDS);
                if (result_message == null) {
                    System.out.println("Time out while wait for columnSum: " + i);
                    System.exit(100);
                }
                sum = sum + result_message.var;
                // System.out.println("Getting columnSum from: " + i);
            }
            return sum;
        } catch (Exception e) {
            System.out.println("columnSum Failed. This should not happen: " + e);
            System.exit(100);
            return -1;
        }
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
     *
     *  Returns the sum of all elements in the first column of the table,
     *  subject to the passed-in predicates.
     */
    @Override
    public long predicatedColumnSum(int threshold1, int threshold2) {
        long sum = 0;
        for (Worker worker : workers) {
            OpMessage m = new OpMessage();
            m.op = 4;
            m.var1 = threshold1;
            m.var2 = threshold2;
            worker.command_queue.add(m);
            // System.out.println("Sending command.");
        }
        try {
            for (int i = 0; i < workers.length; i++) {
                ResultMessage result_message = result_queue.poll(4, TimeUnit.SECONDS);
                if (result_message == null) {
                    System.out.println("Time out while wait for columnSum: " + i);
                    System.exit(100);
                }
                // System.out.println("Receive command from " + i);
                sum = sum + result_message.var;
            }
            return sum;
        } catch (Exception e) {
            System.out.println("predicatedColumnSum Failed. This should not happen: " + e);
            System.exit(100);
            return -1;
        }
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
     *
     *  Returns the sum of all elements in the rows which pass the predicate.
     */
    @Override
    public long predicatedAllColumnsSum(int threshold) {
        long sum = 0;
        for (Worker worker : workers) {
            OpMessage m = new OpMessage();
            m.op = 5;
            m.var1 = threshold;
            worker.command_queue.add(m);
        }
        try {
            for (int i = 0; i < workers.length; i++) {
                ResultMessage result_message = result_queue.poll(4, TimeUnit.SECONDS);
                if (result_message == null) {
                    System.out.println("Time out while wait for columnSum: " + i);
                    System.exit(100);
                }
                sum = sum + result_message.var;
            }
            return sum;
        } catch (Exception e) {
            System.out.println("predicatedAllColumnsSum Failed. This should not happen: " + e);
            System.exit(100);
            return -1;
        }
    }

    /**
     * Implements the query
     *   UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
     *
     *   Returns the number of rows updated.
     */
    @Override
    public int predicatedUpdate(int threshold) {
        int count = 0;
        for (Worker worker : workers) {
            OpMessage m = new OpMessage();
            m.op = 6;
            m.var1 = threshold;
            worker.command_queue.add(m);
        }
        try {
            for (int i = 0; i < workers.length; i++) {
                ResultMessage result_message = result_queue.poll(4, TimeUnit.SECONDS);
                if (result_message == null) {
                    System.out.println("Time out while wait for columnSum: " + i);
                    System.exit(100);
                }
                count = count + (int)result_message.var;
            }
            return count;
        } catch (Exception e) {
            System.out.println("predicatedUpdate Failed. This should not happen: " + e);
            System.exit(100);
            return -1;
        }
    }
}
