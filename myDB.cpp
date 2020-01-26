#include<iostream>
/*
 * myDB class which initialize DB environment and DATABASE
 * myDB constructor which initialize ENV and DATABASE
 * deconstructor to close the environment and database
 */
class myDB {
    private:
        const char *sqldb = "sql5300.db";
        const unsigned int BLOCK_SIZE = 4096;
        Db *myDb = NULL;
        DbEnv *myEnv = NULL;
    public :
        // constructor
        myDB(const char *envHome) 
        {
            std::string myEnvdir = envHome;
            myEnv = new DbEnv(0U);
            myEnv->set_message_stream(&std::cout);
            myEnv->set_error_stream(&std::cerr);
            myEnv->open(myEnvdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);
            myDb = new Db(myEnv, 0);
            myDb->set_message_stream(myEnv->get_message_stream());
            myDb->set_error_stream(myEnv->get_error_stream());
            myDb->set_re_len(BLOCK_SIZE); // Set record length to 4K
            myDb->open(NULL, sqldb, NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644); // Erases anything already there
        }

        // destructor
        ~myDB()
        {
            myDb->close(0);
            delete myDb;
            myEnv->close(0);
            delete myEnv;
        }
};
















