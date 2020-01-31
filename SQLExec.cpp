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
Indices* SQLExec::indices = nullptr;

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
                    case ColumnAttribute::BOOLEAN:
                        out << (value.n == 0 ? "false" : "true");
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
    // Initialize static system table object (singleton) to be used through out the execution of query.
    if (tables == NULL) 
        SQLExec::tables = new Tables();
    if(indices == NULL)
        SQLExec::indices = new Indices(); // Where are these freed - memory leak potential?

    // Determine which type of SQL statement it is
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
                // Here would be INSERT, SELECT, &c
        }
    } catch (DbRelationError& e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

/* 
 * Provided ColumnAttribute on the basis col definition privided by 
 * statement in create_table method.
 */
void SQLExec::column_definition(const ColumnDefinition *col, Identifier& column_name, ColumnAttribute& column_attribute) {
    column_name = string(col->name);
    switch (col->type) {
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        default:
            throw SQLExecError(" Unrecognized data type");
    }
}

/*
 * Create Statement to Create SQL Objects Table.
 */
QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch(statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        case CreateStatement::kIndex:
            return create_index(statement);
        default:
            throw SQLExecError(" Only CREATE TABLE and CREATE INDEX are implemented");
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
    Identifier cn;
    ColumnAttribute ca;

    // get all the columns and column attributes (data type)
    for(ColumnDefinition *col : *statement->columns){
        column_definition(col, cn, ca);
        columnNames.push_back(cn);
        columnAttributes.push_back(ca);
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
                // Note no support for BOOLEAN yet - would need to modify the parser to support it
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
 * Create index for tables:
 * 1. retrievs column name for the table mentioned in statement. 
 * 2. create row for _indices table and validate if index column exist in the table..
 * 3. call DBIndex create method (not use for right).
 */
QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    Identifier index_name = statement->indexName;
    Identifier table_name = statement->tableName;
    DbRelation& table = SQLExec::tables->get_table(statement->tableName);
    const ColumnNames& columnNames = table.get_column_names();

    ValueDict row;
    row["table_name"] = Value(table_name);
    row["index_name"] = Value(statement->indexName);
    row["index_type"] = Value(statement->indexType);
    // Note that the ValueDict stores boolean values internally as 1 or 0 ints
    // Also note that compare returns 0 for no diffs
    row["is_unique"] = Value(string(statement->indexType).compare(string("BTREE")) == 0? 1 : 0);
    uint i = 1;
    for(auto const& col : *statement->indexColumns){
        // Comapre if the rows exist in table on which we are creating index.
        if (find(columnNames.begin(), columnNames.end(), col) == columnNames.end())
            throw SQLExecError(" Cannot create index on non existing column in table");
        row["seq_in_index"] = Value(i++);
        row["column_name"] = Value(col);
        indices->insert(&row);
    }

    DbIndex &index = indices->get_index(table_name, index_name); 
    index.create();
    return new QueryResult("created index "+ index_name); 
}

/*
 * calls the respective drop method on basis for drop command.
 */
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch(statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            throw SQLExecError(" Only DROP TABLE and DROP INDEX are implemented");
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

    // Prevent droping _tables or _columns
    if(table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME) {
        throw SQLExecError(" Can't delete schema tables");
    } 
    // also prevent droping _indices
    else if(table_name == Indices::TABLE_NAME)
        throw SQLExecError(" Can't delete schema tables: use drop index to delete an index");
    // and confirm that such a table actually exists!
    else if(!table_exists(table_name))
        throw SQLExecError(" Can't delete non-extant table");

    //delete index entries from _indices table for this table name, 
    IndexNames index_names = indices->get_index_names(table_name); // get all the indexs for this table.
    for(Identifier index_name : index_names)
        delete_index_table_row( table_name, index_name);

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
 * calls delete_index_table method to delete rows from _indices table.
 * extracts table name and index name from statement.
 */
QueryResult *SQLExec::drop_index(const DropStatement *statement) {

    Identifier index_name = statement->indexName;
    Identifier table_name = statement->name;

    // Check that the index exists
    if(!index_exists(table_name,index_name))
        throw SQLExecError(" Can't drop non-extant index");

    delete_index_table_row(table_name, index_name);
    return new QueryResult(string("dropped ") + index_name + " From "+ table_name);
}

/*
 * delete entries from _indices table from specific table name, the index name..
 * @params: table name and index name.
 */
void SQLExec::delete_index_table_row(Identifier table_name, Identifier index_name) {
    // Dropping the index 
    DbIndex &dbIndex = indices->get_index(table_name, index_name);
    dbIndex.drop();

    // deleting row from indices table
    ValueDict where;
    where["table_name"] = Value(table_name);
    where["index_name"] = Value(index_name);
    Handles* handles = indices->select(&where);    
    for (auto const& handle: *handles) 
        indices->del(handle);

    delete handles;
}

/* 
 * Show entries from _tables / _columns
 * Support ( show tables, show columns, show index)
 */
QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch(statement->type){
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            throw SQLExecError(" not implemented");
    } 
    return NULL; 
}



/*
 * show index: show index from _indices
 * 1. get the column name and attributes from _indices table using table object.
 * 2. get the entries from _indices table by getting handles of select method.
 */
QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    return show_index(statement->tableName);
}

/*
 * show index: overload to allow passing an Identity for the table name rather than a statement struct
 * This one has most of the orignal content of show_index, while the original version just passes 
 * and Identity to this one
 */
QueryResult* SQLExec::show_index(const Identifier table_name) {

    // Check that the table actually exists
    if(!table_exists(table_name))
        throw SQLExecError("Error: No index on non-extant table");

    ColumnNames *resultsColNames = new ColumnNames();
    ColumnAttributes *resultsColAttribs = new ColumnAttributes();
    tables->get_columns(Indices::TABLE_NAME, *resultsColNames, *resultsColAttribs);

    ValueDict where;
    where["table_name"] = Value(table_name);

    Handles *handles = indices->select(&where);

    ValueDicts *rows = new ValueDicts;
    for(auto const& handle : *handles) {
        ValueDict *row = indices->project(handle, resultsColNames);
        rows->push_back(row);
    }

    delete handles;
    return new QueryResult(resultsColNames, resultsColAttribs, rows,"successfully returned " + to_string(rows->size()) + " rows");
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

    // Iterate over the handles to get all the rows, add each to the rows ValueDicts vecotr
    for(Handle handle : *handles) {
        ValueDict *row = tables->project(handle, resultsColNames);
        if(row->at("table_name") != Value("_tables") && row->at("table_name") != Value("_columns") && row->at("table_name") != Value("_indices")) 
            rows->push_back(row);
    }
    delete handles;

    // Pass the names, attributes, rows, and a message to the query result constructor
    // Query result has an ostream operator for producing text output
    return new QueryResult(resultsColNames, resultsColAttribs, rows,"successfully returned " + to_string(rows->size()) + " rows");
}

/*
 * table_exists: check the existance of a table
 * looks for its presence in the _tables table by calling
 * show_tables() and inspecting the results
 * (Room for improvment here!)
 * used for prettier error handling when the user does something wrong,
 * e.g. tries to drop a table that dosn't exist
 */
bool SQLExec::table_exists(Identifier table_name_to_check) {

    QueryResult* tables_result = show_tables();
    //ValueDicts* rows = tables_result->get_rows();
    bool toReturn = false;

    // Iterate through the rows and check for match
    for(auto const& r : *(tables_result->get_rows())) {
        if(r->at("table_name") == table_name_to_check) {
            toReturn = true;
            break;
        }
    }
    delete tables_result;
    return toReturn;
}

/*
 * index_exists: check the existance of an index
 * first calls table_exists to check for a valid table
 * then looks for the existance of the index by calling
 * show_index and looks for the cor
 * used for prettier error handling when the user tries
 * to drop an index that dosn't exist
 */
bool SQLExec::index_exists(Identifier table_name, Identifier index_name) {

    bool toReturn = false;

    // Check that table exists
    if(table_exists(table_name)) {
        // Check if the index eixst by calling show_index on that table
        QueryResult* indices_result = show_index(table_name);
        // check if the index name exists
        for(auto const& r : *(indices_result)->get_rows()) {
            if(r->at("index_name") == index_name) {
                toReturn = true;
                break;
            }
        }
        delete indices_result;
        return toReturn;
    }
    else
        return false;
}

/*
 * show columns: show entries from _columns table.
 * 1. get all the columns and column attributes of _columns table.
 * 2. get _column table object and shows all the entries supporting where clause.
 */
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {

    // verify that the table specifed exists
    // Note that showing columns from schema tables is allowed
    Identifier table_name = statement->tableName;
    if(!table_exists(table_name) && 
            !(table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME 
                || table_name == Indices::TABLE_NAME))
        throw SQLExecError(" No columns to show for non-extant table");

    ColumnNames *resultsColNames = new ColumnNames();
    ColumnAttributes *resultsColAttribs = new ColumnAttributes();

    tables->get_columns(Columns::TABLE_NAME, *resultsColNames, *resultsColAttribs);

    // use a WHERE to only get the rows from _columns that match the given table name
    ValueDict where;
    where["table_name"] = Value(table_name);

    DbRelation &column_table = tables->get_table(Columns::TABLE_NAME);
    Handles *handles = column_table.select(&where); // get all the handles supporting where clause.

    // Iterate through the handles to get rows using the project function from MS2
    // and add rows to the ValueDicts vecotr
    ValueDicts *rows = new ValueDicts;
    for(Handle handle : *handles) {
        ValueDict *row = column_table.project(handle, resultsColNames);
        rows->push_back(row);
    }
    delete handles;

    return new QueryResult(resultsColNames, resultsColAttribs, rows,"successfully returned " + to_string(rows->size()) + " rows");
}
