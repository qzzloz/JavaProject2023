package database;

import java.util.ArrayList;
import java.util.List;

class ColumnImpl implements Column{

    String header;
    List<String> cell = new ArrayList<>();
    @Override
    public String getHeader() {
        return header;
    }

    @Override
    public String getValue(int index) {
        return cell.get(index);
    }

    @Override
    public <T extends Number> T getValue(int index, Class<T> t) {
        return null;
    }

    @Override
    public void setValue(int index, String value) {
        cell.set(index, value);
    }

    @Override
    public void setValue(int index, int value) {
        cell.set(index, String.valueOf(value));
    }

    @Override
    public int count() {
        return cell.size();
    }

    @Override
    public void show() {

    }

    @Override
    public boolean isNumericColumn() {
        return false;
    }

    @Override
    public long getNullCount() {
        int cnt=0;
        for(int i=0; i<cell.size(); i++){
            if(cell.get(i).equals("null")) cnt++;
        }
        return cnt;
    }
}
