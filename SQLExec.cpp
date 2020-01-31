/**
 * @file SQLExec.cpp - implementation of SQLExec class 
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Summer 2018"
 */

#include "SQLExec.h"
using namespace std;
using namespace hsql;

// define static data
Tables* SQLExec::tables = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

/*
 * Destructor: clean up all the query result data from all the rows
 * after printing the rows.
 */ 
QueryResult::~QueryResult() {
    if (column_names != NULL)
        delete column_names;
    if (column_attributes != NULL)
        delete column_attributes;
    if (rows != NULL)
        for(ValueDict *row: *rows)
            delete row;
    delete rows;
}

/*
 * This method exceute all the query of the basis of statement type.
 * Currently Support : Create, Drop, Show (Table)
 */
QueryResult *SQLExec::execute(const SQLStatement *statement) throw(SQLExecError) {
    // Initialize static system table object to be used through out the execution of query.
    if (tables == NULL) 
        SQLExec::tables = new Tables();

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError& e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

/*
 * Duplicate to getColumnAttributes Not getting used in my implementation.
 */
void SQLExec::column_definition(const ColumnDefinition *col, Identifier& column_name,
        ColumnAttribute& column_attribute) {
    throw SQLExecError("not implemented");  // FIXME
}

/* 
 * Provided ColumnAttribute on the basis col definition privided by 
 * statement in create_table method.
 */
ColumnAttribute getColumnAttributes(ColumnDefinition *col){
    ColumnAttribute *ct = new ColumnAttribute();
    switch (col->type) {
        case ColumnDefinition::INT:
            ct->set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            ct->set_data_type(ColumnAttribute::TEXT);
            break;
        default:
            throw SQLExecError("unrecognized data type");
    }
    return *ct;
}

/*
 * Create Statement to Create SQL Objects Table.
 */
QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch(statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        default:
            return new QueryResult("Only CREATE TABLE are implemented");
    }
}

/*
 * Create table:
 * 1. get all the columns from statement and convert column data type into column attributes.
 * 2. insert table name into _tables schema system table 
 * 3. insert entry into _columns schema table.
 * 4. create berkley db file of the table.
 * 5. if any exceptions happens remove all the respective entry from all the schema tables.
 */
QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    Identifier table_name = statement->tableName;
    ColumnNames columnNames;
    ColumnAttributes columnAttributes;

    // get all the columns and column attributes.
    for(ColumnDefinition *col : *statement->columns){
        columnNames.push_back(col->name);
        columnAttributes.push_back(getColumnAttributes(col));
    }

    // insert values into _table schema 
    ValueDict row;
    row["table_name"] = Value(table_name);
    Handle t_handle = tables->insert(&row);

    // insert values into _columns schema table
    try {
        Handles col_handles;  // this handle will be used to delete added columns if any exception happens ie, duplicate column case
        DbRelation &column_table = tables->get_table(Columns::TABLE_NAME);
        try {
            for(uint i=0; i< columnNames.size(); i++){
                row["column_name"] = Value(columnNames[i]);
                row["data_type"] = Value(columnAttributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                col_handles.push_back(column_table.insert(&row)); // storing handles into col_handles to remove & adding _columns 
            }

            // Create Table Object and then create its physical  berkley db file.
            DbRelation& table = SQLExec::tables->get_table(table_name);
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();
        } catch(exception& e){
            // remove from _columns if exception occurs 
            try {
                for(auto const &handle : col_handles)
                    column_table.del(handle);
            } catch(exception& e) { }
            throw;
        } 
    }catch(exception& e) {
        try{
            SQLExec::tables->del(t_handle);  // remove entry from _tables,
        } catch(exception& e) { }
        throw;
    }

    return new QueryResult("created " + table_name);
}

/*
 * calls the respective drop method on basis for drop command.
 */
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch(statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        default:
            return new QueryResult("Only DROP TABLE are implemented");
    }
}

/*
 * Drop Table: drops the table.
 * 1. should not drop schema tables.
 * 2. delete entry from _column table and then from _table.
 * 3. delete berkley db file.
 */
QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    Identifier table_name = statement->name;
    if(table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME) {
        return new QueryResult("you can't delete shema tables"); // FIXME
    } 

    DbRelation& table = SQLExec::tables->get_table(table_name);

    // delete entries from _columns tables. 
    ValueDict where;
    where["table_name"] = Value(table_name);

    // get _column table object.
    DbRelation& _column = tables->get_table(Columns::TABLE_NAME);

    Handles* handles = _column.select(&where);
    for(Handle handle : *handles){
        _column.del(handle);
    }
    delete handles;

    // Delete physical berkley db file.
    table.drop();

    // delete entries from _tables tables. deletes entry from table cache as well.
    handles = tables->select(&where);
    for(Handle handle : *handles){
        tables->del(handle);
    }

    delete handles;
    return new QueryResult(string("dropped ") + table_name);
}

/* 
 * Show entries from _tables / _columns
 * Support ( show tables, show columns)
 */
QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch(statement->type){
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        default:
            return new QueryResult("not implemented");
            //throw SQLExceError("unrecognised SHOW type");
    } 
    return NULL; 
}

/*
 * show tables : show entries of _tables schema system table.
 * 1. get all the columns and column attributes of _tables table.
 * 2. get all the entries from _table and only show table name which are not _tables or _columns
 */
QueryResult *SQLExec::show_tables() {
    ColumnNames *resultsColNames = new ColumnNames();
    ColumnAttributes *resultsColAttribs = new ColumnAttributes();

    // get column names and column attributes of _tables
    tables->get_columns(Tables::TABLE_NAME, *resultsColNames, *resultsColAttribs);

    Handles *handles = tables->select();  // get handles of all the tables entries from tables.
    ValueDicts *rows = new ValueDicts();
    for(Handle handle : *handles) {
        ValueDict *row = tables->project(handle, resultsColNames);
        if(row->at("table_name") != Value("_tables") && row->at("table_name") != Value("_columns")) 
            rows->push_back(row);
    }

    delete handles;
    return new QueryResult(resultsColNames, resultsColAttribs, rows,"successfully returned " + to_string(rows->size()) + " rows");
}

/*
 * show columns: show entries from _columns table.
 * 1. get all the columns and column attributes of _columns table.
 * 2. get _column table object and shows all the entries supporting where clause.
 */
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    ColumnNames *resultsColNames = new ColumnNames();
    ColumnAttributes *resultsColAttribs = new ColumnAttributes();

    tables->get_columns(Columns::TABLE_NAME, *resultsColNames, *resultsColAttribs);

    ValueDict where;
    where["table_name"] = Value(statement->tableName);

    DbRelation &column_table = tables->get_table(Columns::TABLE_NAME);
    Handles *handles = column_table.select(&where); // get all the handles supporting where clause.

    ValueDicts *rows = new ValueDicts;
    for(Handle handle : *handles) {
        ValueDict *row = column_table.project(handle, resultsColNames);
        rows->push_back(row);
    }

    delete handles;
    return new QueryResult(resultsColNames, resultsColAttribs, rows,"successfully returned " + to_string(rows->size()) + " rows");
}
