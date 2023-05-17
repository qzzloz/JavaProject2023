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
        Class[] interfaces = getClass().getInterfaces();
        System.out.println("<"+interfaces[0].getName()+"@"+getClass().hashCode()+">");
        System.out.println("RangeIndex: "+(getRowCount()-1)+" entries, 0 to "+(getRowCount()-2));
        System.out.println("Data columns (total "+getColumnCount()+" columns): ");

        String[] max = {"#", "Columns", "Non-Null Count", "Dtype"};
        String[] Dtype = new String[column.size()];

        if(getColumnCount() > 9) max[0] = "10";

        //Column열
        for(int i=0; i<column.size(); i++){
            if(column.get(i).getValue(0).length() > max[1].length()) max[1] = column.get(i).getValue(0);
        }

        //Non-null Count열
        int[] cntNonNull = new int[column.size()];
        for (int i = 0; i < column.size(); ++i) {
            int cnt = 0;
            for (int j = 0; j < column.get(0).cell.size(); j++) {
                if(column.get(i).getValue(j).equals("null")) cnt++;
            }
            cntNonNull[i] = (getRowCount()-1) - cnt;
        }

        //Dtype열
        int cntInt=column.size();
        for(int i=0; i<column.size(); i++) {
            try{
                if (column.get(i).getValue(1).equals("null")){
                    for(int j=1; j<column.get(0).cell.size(); j++) {
                        if (!column.get(i).getValue(j).equals("null")) {
                            int isNumber = Integer.parseInt(column.get(i).getValue(j));
                            Dtype[i] = "int";
                            break;
                        }
                    }
                }
                else{
                    int isNumber = Integer.parseInt(column.get(i).getValue(1));
                    Dtype[i] = "int";
                }
            }
            catch (NumberFormatException e){    //cell이 int가 아닐 때
                Dtype[i] = "String";
            }
        }
        for(int i=0; i<column.size(); ++i) {
            if(Dtype[i].equals("String")) {
                max[3] = "String";
                cntInt--;
            }
        }
        //출력
        System.out.println(String.format("%"+max[0].length()+"s | "+"%"+max[1].length()+"s | "
                +"%"+max[2].length()+"s | "+"%"+max[3].length()+"s | ", "#", "Column", max[2], "Dtype"));

        for(int i=0; i<column.size(); i++){
            System.out.println(String.format("%"+max[0].length()+"d | "+"%"+max[1].length()+"s | "
                    +"%"+max[2].length()+"s | "+"%"+max[3].length()+"s | ", i, column.get(i).getValue(0), cntNonNull[i]+" non-null", Dtype[i]));
        }
        System.out.println("dtypes: int("+cntInt+"), String("+(column.size()-cntInt)+")");
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
