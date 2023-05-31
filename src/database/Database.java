package database;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Database {
    // 테이블명이 같으면 같은 테이블로 간주된다.
    private static final Set<Table> tables = new HashSet<>();

    // 테이블 이름 목록을 출력한다.
    public static void showTables() {
        Iterator<Table> iter = tables.iterator();
        while (iter.hasNext()) {
            System.out.println(iter.next().getName());
        }
    }

    /**
     * 파일로부터 테이블을 생성하고 table에 추가한다.
     *
     * @param csv 확장자는 csv로 가정한다.
     *            파일명이 테이블명이 된다.
     *            csv 파일의 1행은 컬럼명으로 사용한다.
     *            csv 파일의 컬럼명은 중복되지 않는다고 가정한다.
     *            컬럼의 데이터 타입은 int 아니면 String으로 판정한다.
     *            String 타입의 데이터는 ("), ('), (,)는 포함하지 않는 것으로 가정한다.
     */
    public static void createTable(File csv) throws FileNotFoundException {
        TableImpl A = new TableImpl();
        A.name = csv.getName().substring(0, csv.getName().lastIndexOf("."));    //테이블명 = 파일명

        try {
            BufferedReader r = new BufferedReader(new FileReader(csv));
            String line = "";

            line = r.readLine();
            String h[] = line.split(",");
            for (int i = 0; i < h.length; i++) {
                ColumnImpl col = new ColumnImpl();
                BufferedReader br = new BufferedReader(new FileReader(csv));
                col.header = h[i];
                line = br.readLine();
                line = br.readLine();
                while (line != null) {
                    String arr[] = line.split(",", -1);
                    if (arr[i].isEmpty() || arr[i].isBlank()) col.cell.add(null);
                    else col.cell.add(arr[i]);
                    line = br.readLine();
                }
                A.column.add(col);
            }

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        tables.add(A);
    }

    // tableName과 테이블명이 같은 테이블을 리턴한다. 없으면 null 리턴.
    public static Table getTable(String tableName) {
        Iterator<Table> iter = tables.iterator();
        while (iter.hasNext()) {
            Table tmp = iter.next();
            if (tmp.getName().equals(tableName)) return tmp;
        }
        return null;
    }

    /**
     * @param byIndexOfColumn 정렬 기준 컬럼, 존재하지 않는 컬럼 인덱스 전달시 예외 발생시켜도 됨.
     * @return 정렬된 새로운 Table 객체를 반환한다. 즉, 첫 번째 매개변수 Table은 변경되지 않는다.
     */
    public static Table sort(Table table, int byIndexOfColumn, boolean isAscending, boolean isNullFirst) {
        for(int i=0; i<table.getColumnCount(); i++){
            for(int j=0; j<table.getRowCount(); j++){
                if(table.getColumn(i).getValue(j) == null) table.getColumn(i).setValue(j, "null");
            }
        }
        String[][] str = new String[table.getRowCount()][table.getColumnCount()];

        int[] nullCnt = new int[(int) table.getColumn(byIndexOfColumn).getNullCount()];
        int[] setOrder = new int[str.length];
        for (int i = 0; i < setOrder.length; i++) setOrder[i] = i;

        for (int i = 0; i < table.getColumnCount(); i++) {
            for (int j = 0; j < table.getRowCount(); j++) {
                str[j][i] = table.getColumn(i).getValue(j);
            }
        }

        int index = 0;
        int startNull = 0, lastNull = str.length - 1;
        for (int i = 0; i < table.getRowCount(); i++) {
            if (str[i][byIndexOfColumn].equals("null")) {
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
        for (int i = 0; i < str.length; i++) {
            copy[i] = str[i][byIndexOfColumn];
        }


        if (isNullFirst && isAscending) {
            startNull = 0;
            lastNull = (int) (table.getColumn(byIndexOfColumn).getNullCount());  //마지막 null의 인덱스는 lastnull-1
            Arrays.sort(copy, lastNull, str.length);    //lastnull 인덱스부터 마지막 인덱스까지 정렬

            for (int i = 0; i < str.length; i++) {
                for (int j = lastNull; j < copy.length; j++) {
                    if (copy[i] == str[j][byIndexOfColumn]) {
                        setOrder[i] = j;
                        break;
                    }
                }
            }
//            for(int i=0; i< copy.length; i++) System.out.println(setOrder[i]);
            for (int i = 0; i < table.getColumnCount(); i++) {
                String[] tmpcol = new String[str.length];
                for (int j = 0; j < str.length; j++) {
                    tmpcol[j] = str[setOrder[j]][i];
                }
                for (int j = 0; j < str.length; j++) {
                    str[j][i] = tmpcol[j];
                }
            }
        } else if ((!isNullFirst && isAscending) || (isNullFirst && !isAscending)) {
            startNull = (int) (str.length - table.getColumn(byIndexOfColumn).getNullCount()); //처음 null의 인덱스
            lastNull = str.length;  //마지막 null의 인덱스는 lastnull-1
//            Arrays.sort(copy, 0, startNull);    //
            index = 0;
            for (int i = 0; i < table.getRowCount(); i++) {
                if (str[i][byIndexOfColumn].equals("null")) {
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

            for (int i = 0; i < str.length; i++) {
                copy[i] = str[i][byIndexOfColumn];
            }
            Arrays.sort(copy, 0, startNull);    //

            for (int i = 0; i < str.length; i++) {
                for (int j = 0; j < startNull; j++) {
                    if (copy[i] == str[j][byIndexOfColumn]) {
                        setOrder[i] = j;
                        break;
                    }
                }
            }
//            for(int i=0; i< copy.length; i++) System.out.println(setOrder[i]);

            if (isNullFirst && !isAscending) {
                for (int i = 0; i < table.getColumnCount(); i++) {
                    String[] tmpcol = new String[str.length];
                    for (int j = 0; j < str.length; j++) {
                        tmpcol[j] = str[setOrder[str.length - 1 - j]][i];
                    }
                    for (int j = 0; j < str.length; j++) {
                        str[j][i] = tmpcol[j];
                    }

                }

            } else {
                for (int i = 0; i < table.getColumnCount(); i++) {
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
        else if (!isNullFirst && !isAscending) {
            startNull = 0;
            lastNull = (int) (table.getColumn(byIndexOfColumn).getNullCount());  //마지막 null의 인덱스는 lastnull-1
            Arrays.sort(copy, lastNull, str.length);    //lastnull 인덱스부터 마지막 인덱스까지 정렬
//            for(int i=0; i<setOrder.length; i++) setOrder[i] = setOrder.length-i-1;

            List<String> list = Arrays.asList(copy);
            Collections.reverse(list);
            copy = list.toArray(copy);

            if (table.getColumn(byIndexOfColumn).getNullCount() != 0) {
                int copyStartnull = (int) (str.length - table.getColumn(byIndexOfColumn).getNullCount());
                for (int i = copyStartnull; i < str.length; i++) setOrder[i] = startNull++;
            }

            startNull = (int) (str.length - table.getColumn(byIndexOfColumn).getNullCount()); //copy 처음 null의 인덱스
            lastNull = (int) (table.getColumn(byIndexOfColumn).getNullCount());  //str 마지막 null의 인덱스는 lastnull-1

            for (int i = 0; i < startNull; i++) {
                for (int j = lastNull; j < str.length; j++) {
                    if (copy[i] == str[j][byIndexOfColumn]) {
                        setOrder[i] = j;
                        break;
                    }
                }
            }
//            for (int i = 0; i < copy.length; i++) System.out.println(setOrder[i]);

            for (int i = 0; i < table.getColumnCount(); i++) {
                String[] tmpcol = new String[str.length];
                for (int j = 0; j < str.length; j++) {
                    tmpcol[j] = str[setOrder[j]][i];
                }
                for (int j = 0; j < str.length; j++) {
                    str[j][i] = tmpcol[j];
                }
            }
        }

        TableImpl tmp = new TableImpl();
        for (int i = 0; i < table.getColumnCount(); i++) {
            ColumnImpl tmpCol = new ColumnImpl();
            tmpCol.header = table.getColumn(i).getHeader();
            for (int j = 0; j < table.getRowCount(); j++) {
                tmpCol.cell.add(str[j][i]);
            }
            tmp.column.add(tmpCol);
        }
        return tmp;
    }
}
