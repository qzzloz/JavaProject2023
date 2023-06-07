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
        try {
            if (cell.get(index).equals(null)) return null;
            if (t == Double.class) {
                return t.cast(Double.valueOf(cell.get(index)));
            } else if (t == Long.class) {
                return t.cast(Long.valueOf(cell.get(index)));
            } else if (t == Integer.class) {
                return t.cast(Integer.valueOf(cell.get(index)));
            } else return null;
        } catch (NumberFormatException e){
            return null;
        }
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
        for (int i = 0; i < cell.size(); i++) {
            try {
                if (!cell.get(i).equals("null")) {
                    int isnum = Integer.parseInt(cell.get(i));
                }
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return true;
    }

    @Override
    public long getNullCount() {
        int cnt=0;
        for(int i=0; i<cell.size(); i++){
            if(cell.get(i) == null ||cell.get(i).equals("null")) cnt++;
        }
        return cnt;
    }
}
