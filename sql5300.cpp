#include <stdio.h>
#include <iostream>
#include <stdlib.h>
#include <string.h>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"
#include "myDB.cpp"
#include <typeinfo>
#include<sstream>

using namespace hsql;
using namespace std;

string execute(const SQLStatement *stmt);
string unparsedCreate(const CreateStatement *stmt);
string unparsedSelect(const SelectStatement *stmt);
string columnDefinitionToString(const ColumnDefinition *col);
string parseExpression(const Expr *exp);
string parseTableReference(const TableRef *fromTable);
string parseOperatorExpression(Expr* expr);


/*
 * Determines the type of statement
 * create or select statement 
 * @param stmt is SQL statement which needs to be translated into string
 * @return string after translation of SQL statement
 */
string execute(const SQLStatement *stmt) {
    switch(stmt->type()){
        case kStmtCreate :
            return unparsedCreate((CreateStatement*) stmt);
        case kStmtSelect:
            return unparsedSelect((SelectStatement*) stmt);
        default :
            return "";
    }
    return "";
}

/*
 * Parse the create statement
 * @param stmt is the SQL create statement 
 * @return string form of SQL create statement
 */
string unparsedCreate(const CreateStatement *stmt) {
    string s("CREATE TABLE ");
    s += stmt->tableName;
    s += " (";
    s += columnDefinitionToString(stmt->columns->at(0)); 
    for(uint i=1; i< stmt->columns->size(); i++)
    {
        s += ", ";
        s += columnDefinitionToString(stmt->columns->at(i));
    }
    return s + ")";
}

/*
 * parse the SQL column definition into string
 * @param column definition in SQL create statement 
 * @return string of column name and it's type 
 */
string columnDefinitionToString(const ColumnDefinition *col) {
    string ret(col->name);
    switch(col->type) {
        case ColumnDefinition::DOUBLE:
            ret += " DOUBLE";
            break;
        case ColumnDefinition::INT:
            ret += " INT";
            break;
        case ColumnDefinition::TEXT:
            ret += " TEXT";
            break;
        default:
            ret += " ...";
            break;
    }
    return ret;
}

/*
 * parse the SQL select statement
 * @param stmt is the SQL select statement which need to be translated into string
 * @return string of SQL select satement by combing whole string together
 */
string unparsedSelect(const SelectStatement *stmt) {
    string s(" SELECT ");
    for(uint i=0; i< stmt->selectList->size(); i++)
    {
        if (i > 0)
            s += ", ";
        s += parseExpression(stmt->selectList->at(i));
    }
    s += " FROM ";
    s += parseTableReference(stmt->fromTable);
    if(stmt->whereClause != NULL)
    {
        s += " WHERE ";
        s += parseExpression(stmt->whereClause);
    }
    return s;
}

/*
 * parse table reference of SQL select statement
 * it checks whether a SQL statement has join
 * It checks whether SQL satement selects multiple columns data with alais 
 * @param SQL select statement which keeps information about joins, columns name with alais of table reference
 * @return string form
 */
string parseTableReference(const TableRef *fromTable)
{
    string ret("");
    if (fromTable->type != kTableJoin)
    {
        if (fromTable->list == NULL || fromTable->list->size() == 0)
        {
            ret += fromTable->name;
            if(fromTable->alias != NULL)
            {
                ret += " AS ";
                ret += string(fromTable->alias);
            }
        }
        else
        {
            for(uint i =0; i< fromTable->list->size();i++)
            {
                if (i > 0)
                    ret += ", ";
                TableRef *tblRef = fromTable->list->at(i);
                ret += tblRef->name;
                if(tblRef->alias != NULL)
                {
                    ret += " AS ";
                    ret += tblRef->getName();
                }
            }
        }
    }
    else
    {
        ret += parseTableReference(fromTable->join->left);
        switch(fromTable->join->type)
        {
            case kJoinLeft:
                ret += " LEFT JOIN ";
                break;
            case kJoinOuter:
                ret += " OUTER JOIN ";
                break;
            case kJoinInner:
                ret += " INNER JOIN ";
                break;
            case kJoinRight:
                ret += " RIGHT JOIN ";
                break;
            case kJoinLeftOuter:
                ret += " LEFT OUTER JOIN ";
                break;                                                                                                                                                case kJoinRightOuter:
                    ret += " RIGHT OUTER JOIN ";
                break;
            case kJoinCross:
                ret += " CROSS JOIN ";
                break;
            case kJoinNatural:
                ret += " NATURAL JOIN ";
                break;
        }
        ret += parseTableReference(fromTable->join->right);
        if (fromTable->join->condition != NULL)
        {
            ret += " ON ";
            ret += parseExpression(fromTable->join->condition);
        }
    }
    return ret;
}

/*
 * parse the operator expresion
 * reads expr, operator and expr2 to completly analyse the expr 
 * @param SQL expression which needs to be translated into string 
 * @return string form
 */
string parseOperatorExpression(const Expr* expr) {
    string ret(" ");
    ret += parseExpression(expr->expr);
    ret += " ";
    switch (expr->opType) 
    {
        case Expr::SIMPLE_OP:
            ret += expr->opChar;
            break;
        default:
            ret += " undefined ";
    }
    ret += " "; 
    ret += parseExpression(expr->expr2);
    return ret;
}

/*
 * parse expr of SQL statement
 * @param is the expr SQL statement
 * @return string form
 */
string parseExpression(const Expr *expr) {
    string ret("");
    switch (expr->type) {
        case kExprStar:
            ret += " * ";
            break;
        case kExprColumnRef:
            if(expr->table != NULL)
            {
                ret += expr->table;
                ret += ".";
                ret += expr->name;
            }
            else
            {
                ret += expr->name;
            }
            break;
        case kExprLiteralFloat:
            ret += to_string(expr->fval);
            break;
        case kExprLiteralInt:
            ret += to_string(expr->ival);
            break;
        case kExprLiteralString:
            ret += expr->name;
            break;
        case kExprOperator:
            ret += parseOperatorExpression(expr);
            break;
        default:
            ret += "";
            break;
    }
    return ret;
}

/*
 * main function from where execution starts
 * created object of myDB class 
 * initialize the db environment
 * initialize the database
 * takes input from user
 * parse the input string into SQL statement
 * pases the SQL statements to execute method 
 * cout the translated string as a desired output
 *
 */
int main (int argc, char *argv[])
{
    if (argc != 2) {
        cerr << "Usage: cpsc5300: dbenvpath" << endl;
        return 1;
    }
    char *envHome = argv[1];
    cout << "(sql5300: running with database environment at " << envHome << ")" << endl;
    myDB b(envHome);  
    while(true)
    {
        std::string query = "";
        std::cout<<"SQL> ";
        getline(std::cin, query);
        if (query == "")
            continue;
        if (query == "quit")
            break;
        try{
            hsql::SQLParserResult* result = hsql::SQLParser::parseSQLString(query);
            if (result->isValid()) {
                for (uint i = 0; i < result->size(); ++i) {
                    std::cout << execute(result->getStatement(i)) <<std::endl;
                }

                delete result;
            } 
            else {
                std::cout<<"Result is not valid"<<std::endl;
            }
        }
        catch(...){
            std::cout<<"default exception"<<std::endl;

        }
    }
    return 4;
}
