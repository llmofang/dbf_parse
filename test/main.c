#define KXVER 3
#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include "bool.h"
#include "dbf.h"
#include "k.h"

int connect_kdb(char* host, int port) {
	int handle = khpun(host, port, "user:pass", 10000);
	if ((!handle) || (handle < 0)) {
		printf("Cannot connect\n");
		exit(1);
	}
	else {
		printf("connected %s:%i\n", host, port);
	}
	return handle;
}

int main(int argc, char* argv[])
{
	if (argc < 4) {
		printf("Usage: progname tablename.dbf host port\n");
		exit(1);
	}

	char* filename = argv[1];
	char* host = argv[2];
	int port = atoi(argv[3]);

	int kdbhandle = connect_kdb(host, port);
	
		DBF_HANDLE dbfhandle = dbf_open(filename, NULL);
	if (dbfhandle == NULL) {
		printf("Open dbf error!\n");
		exit(0);
	}

	size_t old_count = 0;
	size_t new_count = 0;
	char cmd[1024] = { 0 };
	

	while (TRUE) {
		new_count = dbf_getrecordcount(dbfhandle);
		for (; old_count < new_count; old_count++) {
			dbf_setposition(dbfhandle, old_count);
			size_t fields = dbf_getfieldcount(kdbhandle);
			for (int j = 0; j < fields; j++)
			{
				char temp[80] = "";
				dbf_getfield(kdbhandle, dbf_getfieldptr(kdbhandle, j), temp, sizeof(temp), DBF_DATA_TYPE_ANY);
				if (j) printf(";");
				printf(temp);
			}
			printf("\n");
		}
	}

}