package memstore.table;

import memstore.data.CSVLoader;
import memstore.data.DataLoader;
import org.junit.Test;
import java.nio.ByteBuffer;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Tests the correctness of the different table implementations for
 * the `columnSum` query.
 */
public class PutGetTest {
    DataLoader dl;
    int total_row = 20;
    int total_col = 5;

    public PutGetTest() {
        dl = new CSVLoader(
                "src/main/resources/test1.csv",
                5
        );
    }

    @Test
    public void testRowTable() throws IOException {
        RowTable rt = new RowTable();
        CustomTable ct = new CustomTable();
        rt.load(dl);
        ct.load(dl);

        for (int row = 0; row < total_row; row++) {
            for (int col = 0; col < total_col; col++) {
                assertEquals(rt.getIntField(row, col), ct.getIntField(row, col));
            }
        }

        assertEquals(rt.columnSum(), ct.columnSum());
        assertEquals(rt.predicatedAllColumnsSum(-1), ct.predicatedAllColumnsSum(-1));
        assertEquals(rt.predicatedColumnSum(3, 2), ct.predicatedColumnSum(3, 2));
        assertEquals(rt.predicatedUpdate(3), ct.predicatedUpdate(3));
        assertEquals(rt.columnSum(), ct.columnSum());
        assertEquals(rt.predicatedAllColumnsSum(-1), ct.predicatedAllColumnsSum(-1));
        assertEquals(rt.predicatedColumnSum(3, 2), ct.predicatedColumnSum(3, 2));

        int rowId = 4;
        int colId = 0;
        assertEquals(rt.getIntField(rowId, colId), ct.getIntField(rowId, colId));
        rt.putIntField(rowId, colId, 10);
        ct.putIntField(rowId, colId, 10);
        assertEquals(rt.getIntField(rowId, colId), ct.getIntField(rowId, colId));

        for (int row = 0; row < total_row; row++) {
            for (int col = 0; col < total_col; col++) {
                assertEquals(rt.getIntField(row, col), ct.getIntField(row, col));
            }
        }

        assertEquals(rt.columnSum(), ct.columnSum());
        assertEquals(rt.predicatedAllColumnsSum(-1), ct.predicatedAllColumnsSum(-1));
        assertEquals(rt.predicatedColumnSum(3, 2), ct.predicatedColumnSum(3, 2));

        rowId = 4;
        colId = 0;
        assertEquals(rt.getIntField(rowId, colId), ct.getIntField(rowId, colId));
        rt.putIntField(rowId, colId, 2);
        ct.putIntField(rowId, colId, 2);
        assertEquals(rt.getIntField(rowId, colId), ct.getIntField(rowId, colId));

        for (int row = 0; row < total_row; row++) {
            for (int col = 0; col < total_col; col++) {
                assertEquals(rt.getIntField(row, col), ct.getIntField(row, col));
            }
        }

        assertEquals(rt.columnSum(), ct.columnSum());
        assertEquals(rt.predicatedAllColumnsSum(-1), ct.predicatedAllColumnsSum(-1));
        assertEquals(rt.predicatedColumnSum(3, 2), ct.predicatedColumnSum(3, 2));

        rowId = 9;
        colId = 0;
        assertEquals(rt.getIntField(rowId, colId), ct.getIntField(rowId, colId));
        rt.putIntField(rowId, colId, 2);
        ct.putIntField(rowId, colId, 2);
        assertEquals(rt.getIntField(rowId, colId), ct.getIntField(rowId, colId));

        for (int row = 0; row < total_row; row++) {
            for (int col = 0; col < total_col; col++) {
                assertEquals(rt.getIntField(row, col), ct.getIntField(row, col));
            }
        }

        assertEquals(rt.columnSum(), ct.columnSum());
        assertEquals(rt.predicatedAllColumnsSum(-1), ct.predicatedAllColumnsSum(-1));
        assertEquals(rt.predicatedColumnSum(3, 2), ct.predicatedColumnSum(3, 2));

        rowId = 9;
        colId = 0;
        assertEquals(rt.getIntField(rowId, colId), ct.getIntField(rowId, colId));
        rt.putIntField(rowId, colId, 1);
        ct.putIntField(rowId, colId, 1);
        assertEquals(rt.getIntField(rowId, colId), ct.getIntField(rowId, colId));

        for (int row = 0; row < total_row; row++) {
            for (int col = 0; col < total_col; col++) {
                assertEquals(rt.getIntField(row, col), ct.getIntField(row, col));
            }
        }

        assertEquals(rt.columnSum(), ct.columnSum());
        assertEquals(rt.predicatedAllColumnsSum(-1), ct.predicatedAllColumnsSum(-1));
        assertEquals(rt.predicatedColumnSum(3, 2), ct.predicatedColumnSum(3, 2));

        rowId = 3;
        colId = 0;
        rt.putIntField(rowId, colId, 10);
        ct.putIntField(rowId, colId, 10);
        assertEquals(rt.getIntField(rowId, colId), ct.getIntField(rowId, colId));

        for (int row = 0; row < total_row; row++) {
            for (int col = 0; col < total_col; col++) {
                assertEquals(rt.getIntField(row, col), ct.getIntField(row, col));
            }
        }

        assertEquals(rt.columnSum(), ct.columnSum());
        assertEquals(rt.predicatedAllColumnsSum(-1), ct.predicatedAllColumnsSum(-1));
        assertEquals(rt.predicatedColumnSum(3, 2), ct.predicatedColumnSum(3, 2));

        assertEquals(rt.predicatedUpdate(3), ct.predicatedUpdate(3));
        for (int row = 0; row < total_row; row++) {
            for (int col = 0; col < total_col; col++) {
                assertEquals(rt.getIntField(row, col), ct.getIntField(row, col));
            }
        }

        rowId = 3;
        colId = 1;
        rt.putIntField(rowId, colId, 10);
        ct.putIntField(rowId, colId, 10);
        assertEquals(rt.getIntField(rowId, colId), ct.getIntField(rowId, colId));
        for (int row = 0; row < total_row; row++) {
            for (int col = 0; col < total_col; col++) {
                assertEquals(rt.getIntField(row, col), ct.getIntField(row, col));
            }
        }
        assertEquals(rt.predicatedUpdate(100), ct.predicatedUpdate(100));
        assertEquals(rt.columnSum(), ct.columnSum());
        assertEquals(rt.predicatedAllColumnsSum(-1), ct.predicatedAllColumnsSum(-1));
        assertEquals(rt.predicatedColumnSum(3, 2), ct.predicatedColumnSum(3, 2));

        rowId = 3;
        colId = 2;
        rt.putIntField(rowId, colId, 10);
        ct.putIntField(rowId, colId, 10);
        assertEquals(rt.getIntField(rowId, colId), ct.getIntField(rowId, colId));
        for (int row = 0; row < total_row; row++) {
            for (int col = 0; col < total_col; col++) {
                assertEquals(rt.getIntField(row, col), ct.getIntField(row, col));
            }
        }
        assertEquals(rt.predicatedUpdate(100), ct.predicatedUpdate(100));
        assertEquals(rt.predicatedUpdate(3), ct.predicatedUpdate(3));
        assertEquals(rt.columnSum(), ct.columnSum());
        assertEquals(rt.predicatedAllColumnsSum(-1), ct.predicatedAllColumnsSum(-1));
        assertEquals(rt.predicatedColumnSum(3, 2), ct.predicatedColumnSum(3, 2));

        rowId = 3;
        colId = 3;
        rt.putIntField(rowId, colId, 10);
        ct.putIntField(rowId, colId, 10);
        assertEquals(rt.getIntField(rowId, colId), ct.getIntField(rowId, colId));
        for (int row = 0; row < total_row; row++) {
            for (int col = 0; col < total_col; col++) {
                assertEquals(rt.getIntField(row, col), ct.getIntField(row, col));
            }
        }
        assertEquals(rt.predicatedUpdate(100), ct.predicatedUpdate(100));
        assertEquals(rt.predicatedUpdate(3), ct.predicatedUpdate(3));
        assertEquals(rt.columnSum(), ct.columnSum());
        assertEquals(rt.predicatedAllColumnsSum(-1), ct.predicatedAllColumnsSum(-1));
        assertEquals(rt.predicatedColumnSum(3, 2), ct.predicatedColumnSum(3, 2));

        rowId = 3;
        colId = 4;
        rt.putIntField(rowId, colId, 10);
        ct.putIntField(rowId, colId, 10);
        assertEquals(rt.getIntField(rowId, colId), ct.getIntField(rowId, colId));
        for (int row = 0; row < total_row; row++) {
            for (int col = 0; col < total_col; col++) {
                assertEquals(rt.getIntField(row, col), ct.getIntField(row, col));
            }
        }
        assertEquals(rt.predicatedUpdate(100), ct.predicatedUpdate(100));
        assertEquals(rt.columnSum(), ct.columnSum());
        assertEquals(rt.predicatedAllColumnsSum(-1), ct.predicatedAllColumnsSum(-1));
        assertEquals(rt.predicatedColumnSum(3, 2), ct.predicatedColumnSum(3, 2));

        rowId = 4;
        colId = 2;
        rt.putIntField(rowId, colId, 10);
        ct.putIntField(rowId, colId, 10);
        assertEquals(rt.getIntField(rowId, colId), ct.getIntField(rowId, colId));
        for (int row = 0; row < total_row; row++) {
            for (int col = 0; col < total_col; col++) {
                assertEquals(rt.getIntField(row, col), ct.getIntField(row, col));
            }
        }
        assertEquals(rt.predicatedUpdate(0), ct.predicatedUpdate(0));
        assertEquals(rt.predicatedUpdate(3), ct.predicatedUpdate(3));
        assertEquals(rt.columnSum(), ct.columnSum());
        assertEquals(rt.predicatedAllColumnsSum(-1), ct.predicatedAllColumnsSum(-1));
        assertEquals(rt.predicatedColumnSum(3, 2), ct.predicatedColumnSum(3, 2));

        rowId = 4;
        colId = 0;
        rt.putIntField(rowId, colId, 1);
        ct.putIntField(rowId, colId, 1);
        assertEquals(rt.getIntField(rowId, colId), ct.getIntField(rowId, colId));
        for (int row = 0; row < total_row; row++) {
            for (int col = 0; col < total_col; col++) {
                assertEquals(rt.getIntField(row, col), ct.getIntField(row, col));
            }
        }
        assertEquals(rt.predicatedUpdate(0), ct.predicatedUpdate(0));
        assertEquals(rt.predicatedUpdate(3), ct.predicatedUpdate(3));
        assertEquals(rt.columnSum(), ct.columnSum());
        assertEquals(rt.predicatedAllColumnsSum(-1), ct.predicatedAllColumnsSum(-1));
        assertEquals(rt.predicatedColumnSum(3, 2), ct.predicatedColumnSum(3, 2));

        rowId = 4;
        colId = 2;
        rt.putIntField(rowId, colId, 6);
        ct.putIntField(rowId, colId, 6);
        assertEquals(rt.getIntField(rowId, colId), ct.getIntField(rowId, colId));
        for (int row = 0; row < total_row; row++) {
            for (int col = 0; col < total_col; col++) {
                assertEquals(rt.getIntField(row, col), ct.getIntField(row, col));
            }
        }
        assertEquals(rt.predicatedUpdate(0), ct.predicatedUpdate(0));
        assertEquals(rt.predicatedUpdate(3), ct.predicatedUpdate(3));
        assertEquals(rt.columnSum(), ct.columnSum());
        assertEquals(rt.predicatedAllColumnsSum(-1), ct.predicatedAllColumnsSum(-1));
        assertEquals(rt.predicatedColumnSum(3, 2), ct.predicatedColumnSum(3, 2));

        rowId = 4;
        colId = 0;
        rt.putIntField(rowId, colId, 6);
        ct.putIntField(rowId, colId, 6);
        assertEquals(rt.getIntField(rowId, colId), ct.getIntField(rowId, colId));
        for (int row = 0; row < total_row; row++) {
            for (int col = 0; col < total_col; col++) {
                assertEquals(rt.getIntField(row, col), ct.getIntField(row, col));
            }
        }
        assertEquals(rt.predicatedUpdate(0), ct.predicatedUpdate(0));
        assertEquals(rt.predicatedUpdate(3), ct.predicatedUpdate(3));
        assertEquals(rt.columnSum(), ct.columnSum());
        assertEquals(rt.predicatedAllColumnsSum(-1), ct.predicatedAllColumnsSum(-1));
        assertEquals(rt.predicatedColumnSum(3, 2), ct.predicatedColumnSum(3, 2));

        rowId = 4;
        colId = 3;
        rt.putIntField(rowId, colId, 2);
        ct.putIntField(rowId, colId, 2);
        assertEquals(rt.getIntField(rowId, colId), ct.getIntField(rowId, colId));
        for (int row = 0; row < total_row; row++) {
            for (int col = 0; col < total_col; col++) {
                assertEquals(rt.getIntField(row, col), ct.getIntField(row, col));
            }
        }
        assertEquals(rt.predicatedUpdate(0), ct.predicatedUpdate(0));
        assertEquals(rt.predicatedUpdate(3), ct.predicatedUpdate(3));
        assertEquals(rt.columnSum(), ct.columnSum());
        assertEquals(rt.predicatedAllColumnsSum(-1), ct.predicatedAllColumnsSum(-1));
        assertEquals(rt.predicatedColumnSum(3, 2), ct.predicatedColumnSum(3, 2));
    }
}
