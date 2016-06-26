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

	//ignore row 0, begin at row 1
	size_t old_count = 1;
	size_t new_count = 0;
	size_t fields = dbf_getfieldcount(dbfhandle);
	char cmd[1024] = { 0 };	

	while (TRUE) {
		new_count = dbf_getrecordcount(dbfhandle);		
		printf("new_count: %d, old_count: %d\n", new_count, old_count);
		for (; old_count < new_count; old_count++) {
			dbf_setposition(dbfhandle, old_count);
			
			char sym[15] = "";
			char xyjydy[15] = "";
			char xyywlb[15] = "";
			char xywtsl[15] = "";
			char xywtjg[15] = "";
			char xyydh[15] = "";
			char xywtsj[15] = "";
			char xyjlzt[15] = "";
			// not used field
			// char xybybz[15] = "";

			dbf_getfield(dbfhandle, dbf_getfieldptr(dbfhandle, 0), sym, sizeof(sym), DBF_DATA_TYPE_ANY);
			dbf_getfield(dbfhandle, dbf_getfieldptr(dbfhandle, 1), xyjydy, sizeof(xyjydy), DBF_DATA_TYPE_ANY);
			dbf_getfield(dbfhandle, dbf_getfieldptr(dbfhandle, 2), xyywlb, sizeof(xyywlb), DBF_DATA_TYPE_ANY);
			dbf_getfield(dbfhandle, dbf_getfieldptr(dbfhandle, 3), xywtsl, sizeof(xywtsl), DBF_DATA_TYPE_ANY);
			dbf_getfield(dbfhandle, dbf_getfieldptr(dbfhandle, 4), xywtjg, sizeof(xywtjg), DBF_DATA_TYPE_ANY);
			dbf_getfield(dbfhandle, dbf_getfieldptr(dbfhandle, 5), xyydh, sizeof(xyydh), DBF_DATA_TYPE_ANY);
			dbf_getfield(dbfhandle, dbf_getfieldptr(dbfhandle, 6), xywtsj, sizeof(xywtsj), DBF_DATA_TYPE_ANY);
			dbf_getfield(dbfhandle, dbf_getfieldptr(dbfhandle, 7), xyjlzt, sizeof(xyjlzt), DBF_DATA_TYPE_ANY);

			sprintf(cmd, "upd[`nqxyxx;([]time:enlist .z.Z; sym:enlist `%s;xyjydy: enlist `%s; xyywlb:enlist `%s; xywtsl:enlist %s; xywtjg:enlist %sf; xyydh:enlist `%s; xywtsj:enlist `%s; xyjlzt:enlist %s; xybybz:enlist `unkown)]",
				sym, xyjydy, xyywlb, xywtsl, xywtjg, xyydh, xywtsj, xyjlzt);

			printf("%s\n", cmd);

			K result = k(-kdbhandle, cmd, (K)0);
			if (!result) {
				printf("Kdb+ Inserting: Network Error ... \n");
			}
			else if (result->t == -128) {
				printf("Kdb+ Inserting: Plus Server Error ... \n");
			}
			else {//7
				printf("Kdb+ Inserting Success ... \n");
			}

		}
	}

}