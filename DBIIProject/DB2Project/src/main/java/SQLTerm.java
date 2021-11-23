public class SQLTerm {
    public String _strTableName;
    public String _strColumnName;
    public String _strOperator;
    public Object _objValue;

    public SQLTerm() {
        this._strTableName = new String();
        this._strColumnName = new String();
        this._strOperator = new String();
        this._objValue = new Object();
    }
}
