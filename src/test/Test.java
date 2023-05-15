package test;

import database.Database;
import database.JoinColumn;
import database.Table;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

public class Test {
    public static void main(String[] args) throws FileNotFoundException {
//        1) CSV 파일로부터 테이블 객체 생성
        Database.createTable(new File("rsc/authors.csv"));
        Database.createTable(new File("rsc/editors.csv"));
        Database.createTable(new File("rsc/translators.csv"));
        Database.createTable(new File("rsc/books.csv"));

//        2) 데이터베이스의 테이블 목록을 출력
        Database.showTables();

//        3) 데이터베이스로부터 테이블을 얻는다.
        Table books = Database.getTable("books");
        Table authors = Database.getTable("authors");
        Table editors = Database.getTable("editors");
        Table translators = Database.getTable("translators");

        Table testTable = books;

//        4) 테이블 내용을 출력한다.
//        testTable.show();

//        5) 테이블 요약 정보를 출력한다.
//        testTable.describe();

        Table headTable;

//        5) 처음 5줄 출력 (새 테이블)
//        testTable.head().show();
//        headTable = testTable.head();
//        System.out.println("identity test for head(): " + (testTable.equals(headTable) ? "Fail" : "Pass"));

//        6) 지정한 처음 n줄 출력 (새 테이블)
//        testTable.head(10).show();
//        headTable = testTable.head(10);
//        System.out.println("identity test for head(n): " + (testTable.equals(headTable) ? "Fail" : "Pass"));

        Table tailTable;

//        7) 마지막 5줄 출력 (새 테이블)
//        testTable.tail().show();
//        tailTable = testTable.tail();
//        System.out.println("identity test for tail(): " + (testTable.equals(tailTable) ? "Fail" : "Pass"));

//        8) 지정한 마지막 n줄 출력 (새 테이블)
//        testTable.tail(10).show();
//        tailTable = testTable.tail(10);
//        System.out.println("identity test for tail(n): " + (testTable.equals(tailTable) ? "Fail" : "Pass"));

        Table selectedRowsTable;

//        9) 지정한 행 인덱스 범위(begin<=, <end)의 서브테이블을 얻는다. (새 테이블), 존재하지 않는 행 인덱시 전달시 예외발생해도 됨.
//        testTable.selectRows(0, 5).show();
//        selectedRowsTable = testTable.selectRows(0, 5);
//        System.out.println("identity test for selectRows(range): " + (testTable.equals(selectedRowsTable) ? "Fail" : "Pass"));

//        10) 지정한 행 인덱스로만 구성된 서브테이블을 얻는다. (새 테이블), 존재하지 않는 행 인덱시 전달시 예외발생해도 됨.
//        testTable.selectRowsAt(7, 0, 4).show();
//        selectedRowsTable = testTable.selectRowsAt(7, 0, 4);
//        System.out.println("identity test for selectRowsAt(indices): " + (testTable.equals(selectedRowsTable) ? "Fail" : "Pass"));

        Table selectedColumnsTable;

//        11) 지정한 열 인덱스 범위(begin<=, <end)의 서브테이블을 얻는다. (새 테이블), 존재하지 않는 열 인덱시 전달시 예외발생해도 됨.
//        testTable.selectColumns(0, 4).show();
//        selectedColumnsTable = testTable.selectColumns(0, 4);
//        System.out.println("identity test for selectColumns(range): " + (testTable.equals(selectedColumnsTable) ? "Fail" : "Pass"));

//        12) 지정한 열 인덱스로만 구성된 서브테이블을 얻는다. (새 테이블), 존재하지 않는 열 인덱시 전달시 예외발생해도 됨.
//        testTable.selectColumnsAt(4, 5, 3).show();
//        selectedColumnsTable = testTable.selectColumnsAt(4, 5, 3);
//        System.out.println("identity test for selectColumnsAt(indices): " + (testTable.equals(selectedColumnsTable) ? "Fail" : "Pass"));

        Table sortedTable;

//        13) 테이블을 기준 열인덱스(5)로 정렬한다. 이 때, 오름차순(true), null값은 나중에(false)(원본 테이블 정렬), 존재하지 않는 열 인덱시 전달시 예외발생해도 됨.
//        testTable.sort(5, true, false).show();
//        sortedTable = testTable.sort(5, true, false);
//        System.out.println("identity test for sort(index, asc, nullOrder): " + (!testTable.equals(sortedTable) ? "Fail" : "Pass"));

//        14) 테이블을 기준 열인덱스(5)로 정렬한다. 이 때, 내림차순(false), null값은 앞에(true)(새 테이블), 존재하지 않는 열 인덱시 전달시 예외발생해도 됨.
//        Database.sort(testTable, 5, false, true).show();
//        sortedTable = Database.sort(testTable, 5, false, true);
//        System.out.println("identity test for Database.sort(index, asc, nullOrder): " + (testTable.equals(sortedTable) ? "Fail" : "Pass"));

        Table rightTable = authors;

//        15) cross join
//        Table crossJoined = testTable.crossJoin(rightTable);
//        crossJoined.show();

//        16) inner join
//        Table innerJoined = testTable.innerJoin(rightTable, List.of(new JoinColumn("author_id", "id")));
//        innerJoined.show();

        rightTable = translators;

//        17) outer join
//        Table outerJoined = testTable.outerJoin(rightTable, List.of(new JoinColumn("translator_id", "id")));
//        outerJoined.show();

//        18) full outer join
//        Table fullOuterJoined = testTable.fullOuterJoin(rightTable, List.of(new JoinColumn("translator_id", "id")));
//        fullOuterJoined.show();

//        19) 조건식을 만족하는 행을 얻는다.
//        testTable.selectRowsBy("title", (String x) -> x.contains("Your")).show();
//        testTable.selectRowsBy("author_id", (Integer x) -> x < 15).show();
//        testTable.selectRowsBy("title", (String x) -> x.length() < 8).show();
//        testTable.selectRowsBy("translator_id", (Object x) -> x == null).show();
//
//        ****************************** test for Column ******************************
        int selectedColumnIndex;
        int selectedRowIndex;
        String selectedColumnName;

//        20) setValue(int index, int value) or setValue(int index, String value)호출 전후 비교
//        System.out.println("*** before setValue ***");
//        selectedColumnIndex = (int) (Math.random() * testTable.getColumnCount());
//        selectedRowIndex = (int) (Math.random() * testTable.getColumn(selectedColumnIndex).count());
//        selectedColumnName = testTable.getColumn(selectedColumnIndex).getHeader();
//        System.out.println("Selected Column: " + selectedColumnName);
//        testTable.selectRowsAt(selectedRowIndex).show();
//        testTable.describe();
//        if (testTable.getColumn(selectedColumnIndex).isNumericColumn())
//            testTable.getColumn(selectedColumnName).setValue(selectedRowIndex, "Sample");
//        else
//            testTable.getColumn(selectedColumnName).setValue(selectedRowIndex, "2023");
//        System.out.println("Column " + selectedColumnName + " has been changed");
//        System.out.println("*** after setValue ***");
//        testTable.selectRowsAt(selectedRowIndex).show();
//        testTable.describe();

//        21) T getValue(int index, Class<T> t) or String getValue(int index) 호출 전후 비교
//        System.out.println("*** before getValue ***");
//        selectedColumnIndex = (int) (Math.random() * testTable.getColumnCount());
//        selectedRowIndex = (int) (Math.random() * testTable.getColumn(selectedColumnIndex).count());
//        selectedColumnName = testTable.getColumn(selectedColumnIndex).getHeader();
//        System.out.println("Selected Column: " + selectedColumnName);
//        testTable.selectRowsAt(selectedRowIndex).show();
//        if (testTable.getColumn(selectedColumnIndex).isNumericColumn()) {
//            // cell 값이 null이면, 예외 발생할 수 있음.
//            double value = testTable.getColumn(selectedColumnName).getValue(selectedRowIndex, Double.class);
//            System.out.println("The numeric value in (" + selectedRowIndex + ", " + selectedColumnIndex + ") is " + value);
//        } else {
//            String value = testTable.getColumn(selectedColumnName).getValue(selectedRowIndex);
//            System.out.println("The string value in (" + selectedRowIndex + ", " + selectedColumnIndex + ") is " + value);
//        }
    }
}
