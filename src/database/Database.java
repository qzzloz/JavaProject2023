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
        while(iter.hasNext()){
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
            for(int i=0; i<h.length; i++){
                ColumnImpl col = new ColumnImpl();
                BufferedReader br = new BufferedReader(new FileReader(csv));
                col.header = h[i];
                line = br.readLine();
                while(line != null){
                    String arr[] = line.split(",", -1);
//                    System.out.println(arr[i]);
                    if(arr[i].isEmpty() || arr[i].isBlank()) col.cell.add("null");
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
        while(iter.hasNext()){
            Table tmp = iter.next();
            if(tmp.getName().equals(tableName)) return tmp;
        }
        return null;
    }

    /**
     * @return 정렬된 새로운 Table 객체를 반환한다. 즉, 첫 번째 매개변수 Table은 변경되지 않는다.
     * @param byIndexOfColumn 정렬 기준 컬럼, 존재하지 않는 컬럼 인덱스 전달시 예외 발생시켜도 됨.
     */
    public static Table sort(Table table, int byIndexOfColumn, boolean isAscending, boolean isNullFirst) {
        return null;
    }
}
