package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import memstore.data.DataLoader;
import memstore.data.ByteFormat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.TreeMap;
import java.util.NavigableMap;
import java.util.List;
import java.io.*;

/**
 * IndexedRowTable, which stores data in row-major format.
 * That is, data is laid out like
 *   row 1 | row 2 | ... | row n.
 *
 * Also has a tree index on column `indexColumn`, which points
 * to all row indices with the given value.
 */
public class IndexedRowTable implements Table {

    int numCols;
    int numRows;
    private TreeMap<Integer, IntArrayList> index;
    private ByteBuffer rows;
    private int indexColumn;

    public IndexedRowTable(int indexColumn) {
        this.indexColumn = indexColumn;
    }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException {
        this.index = new TreeMap<Integer, IntArrayList>();
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
                this.rows.putInt(offset, curRow.getInt(ByteFormat.FIELD_LEN * colId));
            }
            int indexed_int = curRow.getInt(ByteFormat.FIELD_LEN * indexColumn);
            IntArrayList list = index.get(indexed_int);
            if (list == null) {
                index.put(indexed_int, new IntArrayList());
            }
            list = index.get(indexed_int);
            list.add(rowId);
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        return this.rows.getInt(offset);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        if (colId == indexColumn) {
            IntArrayList list = index.get(getIntField(rowId, colId));
            list.rem(rowId);
            IntArrayList list_new = index.get(field);
            if (list_new == null) {
                index.put(field, new IntArrayList());
            }
            list_new = index.get(field);
            list_new.add(rowId);
        }
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        this.rows.putInt(offset, field);
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
        for (int rowId = 0; rowId < numRows; rowId++) {
            sum = sum + getIntField(rowId, 0);
        }
        return sum;
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
        if (indexColumn == 1) {
            long sum = 0;
            NavigableMap<Integer, IntArrayList> map = index.tailMap(threshold1, false);
            for(IntArrayList list: map.values()) {
                for (int row : list) {
                    int col0_field = getIntField(row, 0);
                    int col2_field = getIntField(row, 2);
                    if (col2_field < threshold2) {
                        sum = sum + col0_field;
                    }
                }
            }
            return sum;
        } else if (indexColumn == 2) {
            long sum = 0;
            NavigableMap<Integer, IntArrayList> map = index.headMap(threshold2, false);
            for(IntArrayList list: map.values()) {
                for (int row : list) {
                    int col0_field = getIntField(row, 0);
                    int col1_field = getIntField(row, 1);
                    if (col1_field > threshold1) {
                        sum = sum + col0_field;
                    }
                }
            }
            return sum;
        } else {
            long sum = 0;
            for (int rowId = 0; rowId < numRows; rowId++) {
                int offset0 = ByteFormat.FIELD_LEN * ((rowId * numCols));
                int offset1 = ByteFormat.FIELD_LEN * ((rowId * numCols) + 1);
                int offset2 = ByteFormat.FIELD_LEN * ((rowId * numCols) + 2);
                if (this.rows.getInt(offset1) > threshold1 && this.rows.getInt(offset2) < threshold2) {
                    sum = sum + this.rows.getInt(offset0);
                }
            }
            return sum;
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
        // if (indexColumn == 0) {
        //     long sum = 0;
        //     NavigableMap<Integer, IntArrayList> map = index.tailMap(threshold, false);
        //     for(IntArrayList list: map.values()) {
        //         for (int row : list) {
        //             for (int offset = ByteFormat.FIELD_LEN * (row * numCols); offset < ByteFormat.FIELD_LEN * ((row + 1) * numCols); offset = offset + ByteFormat.FIELD_LEN) {
        //                 sum = sum + this.rows.getInt(offset);
        //             }
        //         }
        //     }
        //     return sum;
        // } else {
            long sum = 0;
            for (int rowId = 0; rowId < numRows; rowId++) {
                int offset0 = ByteFormat.FIELD_LEN * ((rowId * numCols));
                if (this.rows.getInt(offset0) > threshold) {
                    for (int offset = ByteFormat.FIELD_LEN * (rowId * numCols); offset < ByteFormat.FIELD_LEN * ((rowId + 1) * numCols); offset = offset + ByteFormat.FIELD_LEN) {
                        sum = sum + this.rows.getInt(offset);
                    }
                }
            }
            return sum;
        //}
    }

    /**
     * Implements the query
     *   UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
     *
     *   Returns the number of rows updated.
     */
    @Override
    public int predicatedUpdate(int threshold) {
        if (indexColumn == 0) {
            int count = 0;
            NavigableMap<Integer, IntArrayList> map = index.headMap(threshold, false);
            for(IntArrayList list: map.values()) {
                for (int row : list) {
                    count++;
                    putIntField(row, 3, getIntField(row, 3) + getIntField(row, 2));
                }
            }
            return count;
        } else if (indexColumn == 3) {
            int count = 0;
            for (int rowId = 0; rowId < numRows; rowId++) {
                int offset0 = ByteFormat.FIELD_LEN * ((rowId * numCols));
                int offset2 = ByteFormat.FIELD_LEN * ((rowId * numCols) + 2);
                int offset3 = ByteFormat.FIELD_LEN * ((rowId * numCols) + 3);
                if (this.rows.getInt(offset0) < threshold) {
                    int field = this.rows.getInt(offset3) + this.rows.getInt(offset2);
                    this.rows.putInt(offset3, field);
                    count++;
                }
            }
            // Update Index
            this.index = new TreeMap<Integer, IntArrayList>();
            for (int row = 0; row < numRows; row++) {
                IntArrayList list = index.get(getIntField(row, 3));
                if (list == null) {
                    index.put(getIntField(row, 3), new IntArrayList());
                }
                list = index.get(getIntField(row, 3));
                list.add(row);
            }
            return count;
        } else {
            int count = 0;
            for (int rowId = 0; rowId < numRows; rowId++) {
                int offset0 = ByteFormat.FIELD_LEN * ((rowId * numCols));
                int offset2 = ByteFormat.FIELD_LEN * ((rowId * numCols) + 2);
                int offset3 = ByteFormat.FIELD_LEN * ((rowId * numCols) + 3);
                if (this.rows.getInt(offset0) < threshold) {
                    int field = this.rows.getInt(offset3) + this.rows.getInt(offset2);
                    this.rows.putInt(offset3, field);
                    count++;
                }
            }
            return count;
        }
    }
}
