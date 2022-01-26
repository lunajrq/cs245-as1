package memstore.table;

import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Custom table implementation to adapt to provided query mix.
 */
public class CustomTable implements Table {
    protected int numCols;
    protected int numRows;
    protected ByteBuffer rows;

    public CustomTable() { }

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
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
                this.rows.putInt(offset, curRow.getInt(ByteFormat.FIELD_LEN * colId));
            }
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
            int offset = ByteFormat.FIELD_LEN * ((rowId * numCols));
            sum = sum + this.rows.getInt(offset);
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

    /**
     * Implements the query
     *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
     *
     *  Returns the sum of all elements in the rows which pass the predicate.
     */
    @Override
    public long predicatedAllColumnsSum(int threshold) {
        long sum = 0;
        for (int rowId = 0; rowId < numRows; rowId++) {
            if (getIntField(rowId, 0) > threshold) {
                for (int colId = 0; colId < numCols; colId++) {
                    sum = sum + getIntField(rowId, colId);
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
    @Override
    public int predicatedUpdate(int threshold) {
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
