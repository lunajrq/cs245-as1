package memstore.table;

import memstore.data.DataLoader;
import java.nio.IntBuffer;
import java.nio.LongBuffer;

import java.io.IOException;
import memstore.data.ByteFormat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Custom table implementation to adapt to provided query mix.
 */
public class CustomTable implements Table {
    static final int FIELD_MAX = 1024;

    int numCols;
    int numRows;

    /** Sum of col0 (all rows) */
    long col0_sum;
    /** Sum of col2 (per col0 number) */
    // LongBuffer col2_sum;
    /** Sum of all columns (per col0 number) */
    // LongBuffer all_col_sum;
    /** Combined sum of all columns and col2 (per col0 number) */
    LongBuffer all_col2_combined_sum;
    /** Number of columns (per col0 number) */
    IntBuffer count_col0;
    /** Sum of col0 columns (per col1,col2 number) */
    LongBuffer col0_sum_col1_col2;

    IntBuffer t_cache;

    /** */
    IntBuffer predict_update_history;

    /** Column storage of all data */
    IntBuffer rows;



    public CustomTable() {
    }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException {
        this.col0_sum = 0;
        // Buffers are filled with 0s by default
        this.all_col2_combined_sum = LongBuffer.allocate(FIELD_MAX * 2);
        this.count_col0 = IntBuffer.allocate(FIELD_MAX);
        this.col0_sum_col1_col2 = LongBuffer.allocate(FIELD_MAX * FIELD_MAX);
        this.predict_update_history = IntBuffer.allocate(2000);
        this.predict_update_history.position(0);
        this.t_cache = IntBuffer.allocate(FIELD_MAX * 2);
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        this.numRows = rows.size();
        this.rows = IntBuffer.allocate(numRows * numCols);
        this.rows.position(0);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                this.rows.put(curRow.getInt(ByteFormat.FIELD_LEN*colId));
            }
        }

        // Calculate initial col0_sum
        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            int col0 = curRow.getInt(0);
            col0_sum = col0_sum + col0;
        }

        // Calculate initial col2_sum
        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            int col0 = curRow.getInt(0);
            int col2 = curRow.getInt(ByteFormat.FIELD_LEN * 2);
            long sum = all_col2_combined_sum.get(2 * col0 + 1) + col2;
            all_col2_combined_sum.put(2 * col0 + 1, sum);
        }

        // Calculate initial all_col_sum
        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            int col0 = curRow.getInt(0);
            long sum = all_col2_combined_sum.get(col0 * 2);
            for (int colId = 0; colId < numCols; colId++) {
                sum = sum + curRow.getInt(ByteFormat.FIELD_LEN*colId);
            }
            all_col2_combined_sum.put(col0 * 2, sum);
        }

        // Calculate initial count_col0
        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            int col0 = curRow.getInt(0);
            int sum = count_col0.get(col0) + 1;
            count_col0.put(col0, sum);
        }

        // Calculate initial col0_sum_col1_col2
        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            int col0 = curRow.getInt(0);
            int col1 = curRow.getInt(ByteFormat.FIELD_LEN * 1);
            int col2 = curRow.getInt(ByteFormat.FIELD_LEN * 2);
            long sum = col0_sum_col1_col2.get(col1 * FIELD_MAX + col2) + col0;
            col0_sum_col1_col2.put(col1 * FIELD_MAX + col2, sum);
        }

    }

    /**
     *  Note that real_col3 = col3 + t * col2 must always hold
     *  where t is the number of PredictUpdate performed on this row.
     */
    private int getIntField3(int rowId) {
        int col2 = this.rows.get(rowId * numCols + 2);
        int col3 = this.rows.get(rowId * numCols + 3);
        int t = predictUpdateCount(rowId);
        return col3 + t * col2;
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        // System.out.println("row: " + rowId + ", col: " + colId);
        // System.out.println(predictUpdateCount(rowId));
        if (colId != 3) {
            return this.rows.get(rowId * numCols + colId);
        } else {
            return getIntField3(rowId);
        }
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        //System.out.println("Update col: " + colId + "field: " + field);
        if (colId == 0) {
            int col0 = this.rows.get(rowId * numCols);
            if (col0 == field) return;
            int col1 = this.rows.get(rowId * numCols + 1);
            int col2 = this.rows.get(rowId * numCols + 2);
            int col3 = this.rows.get(rowId * numCols + 3);
            // We need to update col0_sum
            col0_sum = col0_sum - col0 + field;
            // We need to update col2_sum
            all_col2_combined_sum.put(col0 * 2 + 1, all_col2_combined_sum.get(col0 * 2 + 1) - col2);
            all_col2_combined_sum.put(field * 2 + 1, all_col2_combined_sum.get(field * 2 + 1) + col2);
            // We need to update all_col_sum
            long row_sum = col1 + col2 + getIntField3(rowId);
            for (int i = 4; i < numCols; i++) {
                row_sum = row_sum + this.rows.get(rowId * numCols + i);
            }
            all_col2_combined_sum.put(2 * col0, all_col2_combined_sum.get(col0 * 2) - row_sum - col0);
            all_col2_combined_sum.put(2 * field, all_col2_combined_sum.get(field * 2) + row_sum + field);
            // We need to update count_col0
            count_col0.put(col0, count_col0.get(col0) - 1);
            count_col0.put(field, count_col0.get(field) + 1);
            // We need to update col0_sum_col1_col2
            long sum = col0_sum_col1_col2.get(col1 * FIELD_MAX + col2);
            col0_sum_col1_col2.put(col1 * FIELD_MAX + col2, sum - col0 + field);
            // We also need to change col3 if necessary
            int new_t = predictUpdateCountByCol0(field);
            int t = predictUpdateCountByCol0(col0);
            int new_col3 = col3 + col2 * (t - new_t);
            this.rows.put(rowId * numCols + 3, new_col3);
            //if(col3 + col2 * t != new_col3 + col2 * new_t) System.out.println("not equal after col0");
            // We could finally update the number
            this.rows.put(rowId * numCols, field);
        } else if (colId == 1) {
            // Update col0_sum_col1_col2
            int col0 = this.rows.get(rowId * numCols);
            int col1 = this.rows.get(rowId * numCols + 1);
            if (col1 == field) return;
            int col2 = this.rows.get(rowId * numCols + 2);
            int sum_old_place_index = col1 * FIELD_MAX + col2;
            int sum_new_place_index = field * FIELD_MAX + col2;
            long sum_old_place = col0_sum_col1_col2.get(sum_old_place_index);
            long sum_new_place = col0_sum_col1_col2.get(sum_new_place_index);
            col0_sum_col1_col2.put(sum_old_place_index, sum_old_place - col0);
            col0_sum_col1_col2.put(sum_new_place_index, sum_new_place + col0);
            // We also need to update all_col_sum
            long sum = all_col2_combined_sum.get(col0 * 2) - col1 + field;
            this.all_col2_combined_sum.put(col0 * 2, sum);
            // Update rows
            this.rows.put(rowId * numCols + colId, field);
        } else if (colId == 2) {
            // Update col0_sum_col1_col2
            int col0 = this.rows.get(rowId * numCols);
            int col1 = this.rows.get(rowId * numCols + 1);
            int col2 = this.rows.get(rowId * numCols + 2);
            if (col2 == field) return;
            int col3 = this.rows.get(rowId * numCols + 3);
            int sum_old_place_index = col1 * FIELD_MAX + col2;
            int sum_new_place_index = col1 * FIELD_MAX + field;
            long sum_old_place = col0_sum_col1_col2.get(sum_old_place_index);
            long sum_new_place = col0_sum_col1_col2.get(sum_new_place_index);
            col0_sum_col1_col2.put(sum_old_place_index, sum_old_place - col0);
            col0_sum_col1_col2.put(sum_new_place_index, sum_new_place + col0);
            // We also need to update all_col_sum
            long sum = all_col2_combined_sum.get(col0 * 2) - col2 + field;
            this.all_col2_combined_sum.put(col0 * 2, sum);
            // We also need to update col2_sum
            sum = all_col2_combined_sum.get(col0 * 2 + 1) - col2 + field;
            this.all_col2_combined_sum.put(col0 * 2 + 1, sum);
            // Update rows
            this.rows.put(rowId * numCols + colId, field);
            // Then we need to update col3 in case we already added to col3 already
            // Note that real_col3 = col3 + t * col2 must always hold
            // where t is the number of PredictUpdate performed on this row.
            int t = predictUpdateCountByCol0(col0);
            int new_col3 = col3 + t * (col2 - field);
            this.rows.put(rowId * numCols + 3, new_col3);
        } else if (colId == 3) {
            int col0 = this.rows.get(rowId * numCols);
            int col2 = this.rows.get(rowId * numCols + 2);
            int col3 = getIntField3(rowId);
            if (col3 == field) return;
            int t = predictUpdateCountByCol0(col0);
            // We also need to update all_col_sum
            long sum = all_col2_combined_sum.get(col0 * 2) - col3 + field;
            this.all_col2_combined_sum.put(col0 * 2, sum);
            // Update rows
            int col3_to_write = field - t * col2;
            this.rows.put(rowId * numCols + colId, col3_to_write);
        } else {
            // Simple case, we only need to update all_col_sum
            int old_field = this.rows.get(rowId * numCols + colId);
            int col0 = this.rows.get(rowId * numCols);
            long sum = all_col2_combined_sum.get(col0 * 2) - old_field + field;
            this.rows.put(rowId * numCols + colId, field);
            this.all_col2_combined_sum.put(col0 * 2, sum);
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
        return col0_sum;
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
        for (int col1 = threshold1 + 1; col1 < FIELD_MAX; col1++) {
            for (int col2 = 0; col2 < threshold2; col2++) {
                sum = sum + col0_sum_col1_col2.get(col1 * FIELD_MAX + col2);
            }
        }
        return sum;
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
        //System.out.println("--------");
        for (int col0 = threshold + 1; col0 < FIELD_MAX; col0++) {
            //System.out.println("col: " + col0 + ", sum: " + all_col_sum.get(col0));

            sum = sum + all_col2_combined_sum.get(col0 * 2);
        }
        //System.out.println("--------");
        return sum;
    }

    private int predictUpdateCount(int rowId) {
        int col0 = this.rows.get(rowId * numCols);
        return predictUpdateCountByCol0(col0);
    }

    private int predictUpdateCountByCol0(int col0) {
        int last_cache = t_cache.get(col0 * 2);
        int count = t_cache.get(col0 * 2 + 1);
        int size = predict_update_history.position();
        for (int i = last_cache; i < size; i++) {
            int threshold = predict_update_history.get(i);
            if (col0 < threshold) count++;
        }
        t_cache.put(col0 * 2, size);
        t_cache.put(col0 * 2 + 1, count);
        return count;
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
        if (predict_update_history.remaining() == 0) {
            // We need to reconcile the result
            for (int rowId = 0; rowId < numRows; rowId++) {
                int col3 = getIntField3(rowId);
                this.rows.put(rowId * numCols + 3, col3);
            }
            predict_update_history.position(0);

            // clearing cache
            for (int i = 0; i < 2 * FIELD_MAX; i++) {
                t_cache.put(i, 0);
            }
        }
        predict_update_history.put(threshold);
        for (int col0 = 0; col0 < threshold; col0++) {
            all_col2_combined_sum.put(col0 * 2, all_col2_combined_sum.get(col0 * 2) + all_col2_combined_sum.get(col0 * 2 + 1));
            count = count + count_col0.get(col0);
        }
        return count;
    }

}