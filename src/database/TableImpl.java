package database;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

class TableImpl implements Table{
    String name;
    List<ColumnImpl> column = new ArrayList<>();

    @Override
    public Table crossJoin(Table rightTable) {
        return null;
    }

    @Override
    public Table innerJoin(Table rightTable, List<JoinColumn> joinColumns) {
        return null;
    }

    @Override
    public Table outerJoin(Table rightTable, List<JoinColumn> joinColumns) {
        return null;
    }

    @Override
    public Table fullOuterJoin(Table rightTable, List<JoinColumn> joinColumns) {
        return null;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void show() {
        int[] max = new int[column.size()];
        for (int i = 0; i < column.size(); ++i) {
            for (int j = 0; j < column.get(0).cell.size(); j++) {
                if (column.get(i).getValue(j).length() > max[i]) max[i] = column.get(i).getValue(j).length();
            }
        }
        for (int i = 0; i < column.get(0).cell.size(); ++i) {
            for (int j = 0; j < column.size(); j++) {
                System.out.print(String.format("%" + max[j] + "s | ", column.get(j).getValue(i)));
            }
            System.out.println();
        }
    }

    @Override
    public void describe() {

    }

    @Override
    public Table head() {
        return null;
    }

    @Override
    public Table head(int lineCount) {
        return null;
    }

    @Override
    public Table tail() {
        return null;
    }

    @Override
    public Table tail(int lineCount) {
        return null;
    }

    @Override
    public Table selectRows(int beginIndex, int endIndex) {
        return null;
    }

    @Override
    public Table selectRowsAt(int... indices) {
        return null;
    }

    @Override
    public Table selectColumns(int beginIndex, int endIndex) {
        return null;
    }

    @Override
    public Table selectColumnsAt(int... indices) {
        return null;
    }

    @Override
    public <T> Table selectRowsBy(String columnName, Predicate<T> predicate) {
        return null;
    }

    @Override
    public Table sort(int byIndexOfColumn, boolean isAscending, boolean isNullFirst) {
        return null;
    }

    @Override
    public int getRowCount() {
        return column.get(0).count();
    }

    @Override
    public int getColumnCount() {
        return column.size();
    }

    @Override
    public Column getColumn(int index) {
        return column.get(index);
    }

    @Override
    public Column getColumn(String name) {
        return null;
    }
}
