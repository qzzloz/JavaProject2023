package database;

import java.util.*;
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
        if(this == null) {
            System.out.println("null");
            return;
        }
        int[] max = new int[column.size()];
        for (int i = 0; i < column.size(); ++i) {
            for (int j = 0; j < column.get(0).cell.size(); j++) {
                if(column.get(i).getValue(j)==null) column.get(i).setValue(j, "null");
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
//        System.out.println(this.toString());
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
        long[] cntNonNull = new long[column.size()];
        for (int i = 0; i < column.size(); ++i) {
            long cnt = column.get(i).getNullCount();
            cntNonNull[i] = (getRowCount()-1) - cnt;
        }

        //Dtype열
        int cntInt=column.size();
        for(int i=0; i<column.size(); i++) {
            try{
                for(int j=1; j<column.get(0).cell.size(); j++) {
                    if (!column.get(i).getValue(j).equals("null")) {
                        int isNumber = Integer.parseInt(column.get(i).getValue(j));
                        Dtype[i] = "int";
                        break;
                    }
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
        TableImpl tmp = new TableImpl();
        for(int i=0; i<column.size(); i++){
            ColumnImpl tmpCol = new ColumnImpl();
            for(int j=0; j<6; j++){
                tmpCol.cell.add(this.column.get(i).getValue(j));
            }
            tmp.column.add(tmpCol);
        }
        return tmp;
    }

    @Override
    public Table head(int lineCount) {
        TableImpl tmp = new TableImpl();
        for(int i=0; i<column.size(); i++){
            ColumnImpl tmpCol = new ColumnImpl();
            if(lineCount<=getRowCount()){
                for(int j=0; j<lineCount+1; j++){
                    tmpCol.cell.add(this.column.get(i).getValue(j));
                }
            } else{
                for(int j=0; j<6; j++){
                    tmpCol.cell.add(this.column.get(i).getValue(j));
                }
            }
            tmp.column.add(tmpCol);
        }
        return tmp;
    }

    @Override
    public Table tail() {
        TableImpl tmp = new TableImpl();
        for(int i=0; i<column.size(); i++){
            ColumnImpl tmpCol = new ColumnImpl();
            tmpCol.cell.add(this.column.get(i).getValue(0));
            for(int j=getRowCount()-5; j<getRowCount(); j++){
                tmpCol.cell.add(this.column.get(i).getValue(j));
            }
            tmp.column.add(tmpCol);
        }
        return tmp;
    }

    @Override
    public Table tail(int lineCount) {
        TableImpl tmp = new TableImpl();
        for(int i=0; i<column.size(); i++){
            ColumnImpl tmpCol = new ColumnImpl();
            if(lineCount<=getRowCount()){
                tmpCol.cell.add(this.column.get(i).getValue(0));
                for(int j=getRowCount()-lineCount; j<getRowCount(); j++){
                    tmpCol.cell.add(this.column.get(i).getValue(j));
                }
            } else{
                for(int j=0; j<getRowCount(); j++){
                    tmpCol.cell.add(this.column.get(i).getValue(j));
                }
            }
            tmp.column.add(tmpCol);
        }
        return tmp;
    }

    @Override
    public Table selectRows(int beginIndex, int endIndex) {
        TableImpl tmp = new TableImpl();
        for (int i = 0; i < column.size(); i++) {
            ColumnImpl tmpCol = new ColumnImpl();
            if (beginIndex != 0) {
                tmpCol.cell.add(this.column.get(i).getValue(0));
            }
            for (int j = beginIndex; j <= endIndex; j++) {
                tmpCol.cell.add(this.column.get(i).getValue(j));
            }
            tmp.column.add(tmpCol);
        }
        return tmp;
    }

    @Override
    public Table selectRowsAt(int... indices) {
        TableImpl tmp = new TableImpl();
        int[] index = new int[indices.length];

        for(int j=0; j<indices.length; j++){
            for(int i=0; i<=getRowCount(); i++){
                if(indices[j]==i) {
                    index[j] = i+1;
                    break;
                }
            }
        }

        for (int i = 0; i < column.size(); i++) {
            ColumnImpl tmpCol = new ColumnImpl();
            tmpCol.cell.add(this.column.get(i).getValue(0));
            for(int j=0; j<index.length; j++){
                tmpCol.cell.add(this.column.get(i).getValue(index[j]));
            }
            tmp.column.add(tmpCol);
        }
        return tmp;
    }

    @Override
    public Table selectColumns(int beginIndex, int endIndex) {
        TableImpl tmp = new TableImpl();
        for (int i = beginIndex; i < endIndex; i++) {
            ColumnImpl tmpCol = new ColumnImpl();
            for (int j = 0; j < getRowCount(); j++) {
                tmpCol.cell.add(this.column.get(i).getValue(j));
            }
            tmp.column.add(tmpCol);
        }
        return tmp;
    }

    @Override
    public Table selectColumnsAt(int... indices) {
        TableImpl tmp = new TableImpl();
        int[] index = new int[indices.length];

        for(int j=0; j<indices.length; j++){
            for(int i=0; i<=getColumnCount(); i++){
                if(indices[j]==i) {
                    index[j] = i;
                    break;
                }
            }
        }

        for (int i = 0; i < index.length; i++) {
            ColumnImpl tmpCol = new ColumnImpl();
            for(int j=0; j<getRowCount(); j++){
                tmpCol.cell.add(this.column.get(index[i]).getValue(j));
            }
            tmp.column.add(tmpCol);
        }
        return tmp;
    }

    @Override
    public <T> Table selectRowsBy(String columnName, Predicate<T> predicate) {
        return null;
    }
    @Override
    public Table sort(int byIndexOfColumn, boolean isAscending, boolean isNullFirst) {
        String[][] str = new String[getRowCount()-1][getColumnCount()];

        int[] nullCnt = new int[(int) column.get(byIndexOfColumn).getNullCount()];
        int[] setOrder = new int[str.length];
        for(int i=0; i<setOrder.length; i++) setOrder[i] = i;

        for(int i=0; i<getColumnCount(); i++){
            for(int j=1; j<getRowCount(); j++){
                str[j-1][i] = column.get(i).getValue(j);
            }
        }

        int index = 0;
        int startNull=0, lastNull=str.length-1;
        for(int i=0; i<getRowCount()-1; i++){
            if(str[i][byIndexOfColumn].equals("null")) {
                nullCnt[index++] = i;
            }
        }

        index = 0;
        for (int i = 0; i < str.length; i++) {
            if (index == nullCnt.length) break;
            for (int j = 0; j < str[0].length; j++) {
                String tmp = str[i][j];
                str[i][j] = str[nullCnt[index]][j];
                str[nullCnt[index]][j] = tmp;
            }
            index++;
        }

        String[] copy = new String[str.length];     //맨 처음 정렬해서 정렬순서 인덱스 만들기 용 배열
        for(int i=0; i< str.length; i++){
            copy[i] = str[i][byIndexOfColumn];
        }
        if(isNullFirst && isAscending) {
            startNull=0;
            lastNull = (int) (column.get(byIndexOfColumn).getNullCount());  //마지막 null의 인덱스는 lastnull-1
            Arrays.sort(copy, lastNull, str.length);    //lastnull 인덱스부터 마지막 인덱스까지 정렬

            for(int i=0; i<str.length; i++){
                for(int j=lastNull; j<copy.length; j++){
                    if(copy[i]==str[j][byIndexOfColumn]){
                        setOrder[i] = j;
                        break;
                    }
                }
            }
//            for(int i=0; i< copy.length; i++) System.out.println(setOrder[i]);
            for(int i=0; i<getColumnCount(); i++){
                String[] tmpcol = new String[str.length];
                for(int j=0; j<str.length; j++){
                    tmpcol[j] = str[setOrder[j]][i];
                }
                for(int j=0; j<str.length; j++){
                    str[j][i] = tmpcol[j];
                }
            }
        }





//        else if(isNullFirst && !isAscending){
//            startNull=0;
//            lastNull = (int) (column.get(byIndexOfColumn).getNullCount());  //마지막 null의 인덱스는 lastnull-1
//            Arrays.sort(copy, lastNull, str.length);    //lastnull 인덱스부터 마지막 인덱스까지 정렬
//            for(int i=0; i<setOrder.length; i++) setOrder[i] = setOrder.length-i-1;
//
//            for(int i=0; i<str.length; i++){
//                for(int j=lastNull; j<copy.length; j++){
//                    if(copy[i]==str[j][byIndexOfColumn]){
//                        setOrder[i] = j;
//                        break;
//                    }
//                }
//            }
//        }





        else if((!isNullFirst && isAscending) || (isNullFirst && !isAscending)) {
            startNull = (int) (str.length - column.get(byIndexOfColumn).getNullCount()); //처음 null의 인덱스
            lastNull = str.length;  //마지막 null의 인덱스는 lastnull-1
//            Arrays.sort(copy, 0, startNull);    //
            index=0;
            for(int i=0; i<getRowCount()-1; i++){
                if(str[i][byIndexOfColumn].equals("null")) {
                    nullCnt[index++] = i;
                }
            }

            index = nullCnt.length - 1;
            for (int i = str.length - 1; i >= 0; i--) {
                if (index < 0) break;
                for (int j = 0; j < str[0].length; j++) {
                    String tmp = str[i][j];
                    str[i][j] = str[nullCnt[index]][j];
                    str[nullCnt[index]][j] = tmp;
                }
                index--;
            }

            for(int i=0; i< str.length; i++){
                copy[i] = str[i][byIndexOfColumn];
            }
            Arrays.sort(copy, 0, startNull);    //

            for(int i=0; i<str.length; i++){
                for(int j=0; j<startNull; j++){
                    if(copy[i]==str[j][byIndexOfColumn]){
                        setOrder[i] = j;
                        break;
                    }
                }
            }
//            for(int i=0; i< copy.length; i++) System.out.println(setOrder[i]);

            if(isNullFirst && !isAscending){
                for(int i=0; i<getColumnCount(); i++){
                    String[] tmpcol = new String[str.length];
                    for(int j=0; j<str.length; j++){
                        tmpcol[j] = str[setOrder[str.length-1-j]][i];
                    }
                    for(int j=0; j<str.length; j++){
                        str[j][i] = tmpcol[j];
                    }
                }
            }

            else {
                for (int i = 0; i < getColumnCount(); i++) {
                    String[] tmpcol = new String[str.length];
                    for (int j = 0; j < str.length; j++) {
                        tmpcol[j] = str[setOrder[j]][i];
                    }
                    for (int j = 0; j < str.length; j++) {
                        str[j][i] = tmpcol[j];
                    }
                }
            }
        }




//
        else if(!isNullFirst &&!isAscending){
            startNull=0;
            lastNull = (int) (column.get(byIndexOfColumn).getNullCount());  //마지막 null의 인덱스는 lastnull-1
            Arrays.sort(copy, lastNull, str.length);    //lastnull 인덱스부터 마지막 인덱스까지 정렬
//            for(int i=0; i<setOrder.length; i++) setOrder[i] = setOrder.length-i-1;

            List<String> list = Arrays.asList(copy);
            Collections.reverse(list);
            copy = list.toArray(copy);

            if(column.get(byIndexOfColumn).getNullCount()!=0){
                int copyStartnull = (int) (str.length-column.get(byIndexOfColumn).getNullCount());
                for(int i=copyStartnull; i<str.length; i++) setOrder[i] = startNull++;
            }

            startNull= (int) (str.length - column.get(byIndexOfColumn).getNullCount()); //copy 처음 null의 인덱스
            lastNull = (int) (column.get(byIndexOfColumn).getNullCount());  //str 마지막 null의 인덱스는 lastnull-1
            for(int i=0; i< copy.length; i++) System.out.println(str[i][byIndexOfColumn]);

            for(int i=0; i<startNull; i++){
                for(int j=lastNull; j< str.length; j++){
                    if(copy[i]==str[j][byIndexOfColumn]){
                        setOrder[i] = j;
                        break;
                    }
                }
            }
            for(int i=0; i< copy.length; i++) System.out.println(setOrder[i]);

            for(int i=0; i<getColumnCount(); i++){
                String[] tmpcol = new String[str.length];
                for(int j=0; j<str.length; j++){
                    tmpcol[j] = str[setOrder[j]][i];
                }
                for(int j=0; j<str.length; j++){
                    str[j][i] = tmpcol[j];
                }
            }
        }
        
        
//
//        if (isNullFirst) {
//            startNull=0;
//            index = 0;
//            for (int i = 0; i < str.length; i++) {
//                if (index == nullCnt.length) break;
//                for (int j = 0; j < str[0].length; j++) {
//                    String tmp = str[i][j];
//                    str[i][j] = str[nullCnt[index]][j];
//                    str[nullCnt[index]][j] = tmp;
//                }
//                index++;
//            }
//            lastNull = (int) (column.get(byIndexOfColumn).getNullCount());
//            if(isAscending){
//                String[] copy = new String[str.length];
//                for(int i=0; i< str.length; i++){
//                    copy[i] = str[i][byIndexOfColumn];
//                }
//
//                Arrays.sort(copy, lastNull, str.length);
//                for(int i=lastNull+1; i<str.length; i++){
//                    for(int j=0; j<copy.length; j++){
//                        if(copy[i]==str[j][byIndexOfColumn]){
//                            setOrder[i] = j;
//                            break;
//                        }
//                    }
//                }
//
//                for(int i=0; i<getColumnCount(); i++){
//                    String[] tmpcol = new String[str.length];
//                    for(int j=lastNull+1; j<str.length; j++){
//                        tmpcol[j] = str[setOrder[j]][i];
//                    }
//                    for(int j=lastNull+1; j<str.length; j++){
//                        str[j][i] = tmpcol[j];
//                    }
//                }
//            }
//            else{
//                String[] copy = new String[str.length];
//                for(int i=0; i< str.length; i++){
//                    copy[i] = str[i][byIndexOfColumn];
//                }
//                Arrays.sort(copy, lastNull, str.length);
//                List<String> list = Arrays.asList(copy);
//                Collections.reverse(list);
//                copy = list.toArray(copy);
//                for(int i=lastNull; i<str.length; i++){
//                    for(int j=0; j<copy.length; j++){
//                        if(str[j][byIndexOfColumn] == copy[i]){
//                            setOrder[i] = j;
//                        }
//                    }
//                }
//                for(int i=0; i<getColumnCount(); i++){
//                    String[] tmpcol = new String[str.length-nullCnt.length];
//                    for(int j=lastNull; j< str.length; j++){
//                        tmpcol[j] = str[setOrder[j]][i];
//                    }
//                    for(int j=lastNull; j< str.length; j++){
//                        str[j][i] = tmpcol[j];
//                    }
//                }
//            }
//        } else {    //null을 뒤로
//            index = nullCnt.length - 1;
//            for (int i = str.length - 1; i >= 0; i--) {
//                if (index < 0) break;
//                for (int j = 0; j < str[0].length; j++) {
//                    String tmp = str[i][j];
//                    str[i][j] = str[nullCnt[index]][j];
//                    str[nullCnt[index]][j] = tmp;
//                }
//                index--;
//            }
//            startNull = (int) (str.length - column.get(byIndexOfColumn).getNullCount());
//
//            if(isAscending){
//                String[] copy = new String[str.length];
//                for(int i=0; i< str.length; i++){
//                    copy[i] = str[i][byIndexOfColumn];
//                }
//
//                Arrays.sort(copy, 0, startNull);
//                for(int i=0; i<str.length-nullCnt.length; i++){
//                    for(int j=0; j<str.length-nullCnt.length; j++){
//                        if(copy[i]==str[j][byIndexOfColumn]){
//                            setOrder[i] = j;
//                            break;
//                        }
//                    }
//                }
//
//                for(int i=0; i<getColumnCount(); i++){
//                    String[] tmpcol = new String[str.length];
//                    for(int j=0; j<str.length-nullCnt.length; j++){
//                        tmpcol[j] = str[setOrder[j]][i];
//                    }
//
//                    for(int j=0; j<str.length-nullCnt.length; j++){
//                        str[j][i] = tmpcol[j];
//                    }
//                }
//            }
//            else{
//                String[] copy = new String[str.length];
//                for(int i=0; i< str.length; i++){
//                    copy[i] = str[i][byIndexOfColumn];
//                }
//
//                System.out.println(startNull);
//                Arrays.sort(copy, 0, startNull);
//                List<String> list = Arrays.asList(copy);
//                Collections.reverse(list);
//                copy = list.toArray(copy);
//                for(int i=0; i< str.length; i++) System.out.println(copy[i]);
//
//
//
//                for(int i=0; i<startNull; i++){
//                    for(int j=0; j<copy.length; j++){
//                        if(str[i][byIndexOfColumn] == copy[j]){
//                            setOrder[i] = j;
//                        }
//                    }
//                }
//                for(int i=0; i<getColumnCount(); i++){
//                    String[] tmpcol = new String[str.length-nullCnt.length];
//                    for(int j=0; j<startNull; j++){
//                        tmpcol[j] = str[setOrder[j]][i];
//                    }
//                    for(int j=0; j<startNull; j++){
//                        str[j][i] = tmpcol[j];
//                    }
//                }
//            }
//        }

        for(int i=0; i<getColumnCount(); i++){
            for(int j=0; j< str.length; j++){
                column.get(i).setValue(j+1, str[j][i]);
            }
        }

        return this;
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
