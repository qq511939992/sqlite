#include "sqlite3.h"
#include <assert.h>
#include <string.h>

/* Hold information about a single WAL frame that was passed to the
** sqlite3_wal_replication.xFrames method implemented in this file.
**
** This is used for test assertions.
*/
typedef struct testWalReplicationFrameInfo testWalReplicationFrameInfo;
struct testWalReplicationFrameInfo {
  unsigned szPage;   /* Number of bytes in the frame's page */
  unsigned pgno;     /* Page number */
  unsigned iPrev;    /* Most recent frame also containing pgno, or 0 if new */

  /* Linked list of frame info objects maintained by testWalReplicationFrames,
  ** head is the newest and tail the oldest. */
  testWalReplicationFrameInfo* pNext;
};

/*
** Global WAL replication context used by this stub implementation of
** sqlite3_wal_replication_wal. It holds a state variable that captures the current
** WAL lifecycle phase and it optionally holds a pointer to a connection in
** follower WAL replication mode.
*/
typedef struct testWalReplicationContextType testWalReplicationContextType;
struct testWalReplicationContextType {
  int eState;          /* Replication state (IDLE, PENDING, WRITING, etc) */
  int eFailing;        /* Code of a method that should fail when triggered */
  int rc;              /* If non-zero, the eFailing method will error */
  int iFailures;       /* Number of times the eFailing method will error */
  sqlite3 *db;         /* Follower connection */
  const char *zSchema; /* Follower schema name */

  /* List of all frames that were passed to the xFrames hook since the last
  ** context reset.
  */
  testWalReplicationFrameInfo *pFrameList;
};
static testWalReplicationContextType testWalReplicationContext;

#define STATE_IDLE      0
#define STATE_PENDING   1
#define STATE_WRITING   2
#define STATE_COMMITTED 3
#define STATE_UNDONE    4
#define STATE_ERROR     5

#define FAILING_BEGIN  1
#define FAILING_FRAMES 2
#define FAILING_UNDO   3
#define FAILING_END    4

/* Reset the state of the global WAL replication context */
static void testWalReplicationContextReset() {
  testWalReplicationFrameInfo *pFrame;
  testWalReplicationFrameInfo *pFrameNext;

  testWalReplicationContext.eState = STATE_IDLE;
  testWalReplicationContext.eFailing = 0;
  testWalReplicationContext.rc = 0;
  testWalReplicationContext.iFailures = 8192; /* Effetively infinite */
  testWalReplicationContext.db = 0;
  testWalReplicationContext.zSchema = 0;

  /* Free all memory allocated for frame info objects */
  pFrame = testWalReplicationContext.pFrameList;
  while( pFrame ){
    pFrameNext = pFrame->pNext;
    sqlite3_free(pFrame);
    pFrame = pFrameNext;
  }

  testWalReplicationContext.pFrameList = 0;
}

/*
** A version of sqlite3_wal_replication.xBegin() that transitions the global
** replication context state to STATE_PENDING.
*/
static int testWalReplicationBegin(
  sqlite3_wal_replication *pReplication, void *pArg
){
  int rc = SQLITE_OK;
  assert( pArg==&testWalReplicationContext );
  assert( testWalReplicationContext.eState==STATE_IDLE
       || testWalReplicationContext.eState==STATE_ERROR
  );
  if( testWalReplicationContext.eFailing==FAILING_BEGIN
   && testWalReplicationContext.iFailures>0
  ){
    rc = testWalReplicationContext.rc;
    testWalReplicationContext.iFailures--;
  }
  if( rc==SQLITE_OK ){
    testWalReplicationContext.eState = STATE_PENDING;
  }
  return rc;
}

/*
** A version of sqlite3_wal_replication.xAbort() that transitions the global
** replication context state to STATE_IDLE.
*/
static int testWalReplicationAbort(
  sqlite3_wal_replication *pReplication, void *pArg
){
  assert( pArg==&testWalReplicationContext );
  assert( testWalReplicationContext.eState==STATE_PENDING );
  testWalReplicationContext.eState = STATE_IDLE;
  return 0;
}

/*
** A version of sqlite3_wal_replication.xFrames() that invokes
** sqlite3_wal_replication_frames() on the follower connection configured in the
** global test replication context (if present).
*/
static int testWalReplicationFrames(
  sqlite3_wal_replication *pReplication, void *pArg,
  int szPage, int nFrame, sqlite3_wal_replication_frame *aFrame,
  unsigned nTruncate, int isCommit
){
  int rc = SQLITE_OK;
  int isBegin = 1;
  int i;
  sqlite3_wal_replication_frame *pNext;
  testWalReplicationFrameInfo *pFrame;

  assert( pArg==&testWalReplicationContext );
  assert( testWalReplicationContext.eState==STATE_PENDING
       || testWalReplicationContext.eState==STATE_WRITING
  );

  /* Save information about these frames */
  pNext = aFrame;
  for (i=0; i<nFrame; i++) {
    pFrame = (testWalReplicationFrameInfo*)(sqlite3_malloc(
        sizeof(testWalReplicationFrameInfo)));
    if( !pFrame ){
	return SQLITE_NOMEM;
    }
    pFrame->szPage = szPage;
    pFrame->pgno = pNext->pgno;
    pFrame->iPrev = pNext->iPrev;
    pFrame->pNext = testWalReplicationContext.pFrameList;
    testWalReplicationContext.pFrameList = pFrame;
    pNext += 1;
  }

  if( testWalReplicationContext.eState==STATE_PENDING ){
    /* If the replication state is STATE_PENDING, it means that this is the
    ** first batch of frames of a new transaction. */
    isBegin = 1;
  }
  if( testWalReplicationContext.eFailing==FAILING_FRAMES
   && testWalReplicationContext.iFailures>0
  ){
    rc = testWalReplicationContext.rc;
    testWalReplicationContext.iFailures--;
  }else if( testWalReplicationContext.db ){
    unsigned *aPgno;
    void *aPage;
    int i;

    aPgno = sqlite3_malloc(sizeof(unsigned) * nFrame);
    if( !aPgno ){
      rc = SQLITE_NOMEM;
    }
    if( rc==SQLITE_OK ){
      aPage = (void*)sqlite3_malloc(sizeof(char) * szPage * nFrame);
    }
    if( !aPage ){
      sqlite3_free(aPgno);
      rc = SQLITE_NOMEM;
    }
    if( rc==SQLITE_OK ){
      for(i=0; i<nFrame; i++){
	aPgno[i] = aFrame[i].pgno;
	memcpy(aPage+(szPage*i), aFrame[i].pBuf, szPage);
      }
      rc = sqlite3_wal_replication_frames(
            testWalReplicationContext.db,
            testWalReplicationContext.zSchema,
            isBegin, szPage, nFrame, aPgno, aPage, nTruncate, isCommit
      );
      sqlite3_free(aPgno);
      sqlite3_free(aPage);
    }
  }
  if( rc==SQLITE_OK ){
    if( isCommit ){
      testWalReplicationContext.eState = STATE_COMMITTED;
    }else{
      testWalReplicationContext.eState = STATE_WRITING;
    }
  }else{
    testWalReplicationContext.eState = STATE_ERROR;
  }
  return rc;
}

/*
** A version of sqlite3_wal_replication.xUndo() that invokes
** sqlite3_wal_replication_undo() on the follower connection configured in the
** global test replication context (if present).
*/
static int testWalReplicationUndo(
  sqlite3_wal_replication *pReplication, void *pArg
){
  int rc = SQLITE_OK;
  assert( pArg==&testWalReplicationContext );
  assert( testWalReplicationContext.eState==STATE_PENDING
       || testWalReplicationContext.eState==STATE_WRITING
       || testWalReplicationContext.eState==STATE_ERROR
  );
  if( testWalReplicationContext.eFailing==FAILING_UNDO
   && testWalReplicationContext.iFailures>0
  ){
    rc = testWalReplicationContext.rc;
    testWalReplicationContext.iFailures--;
  }else if( testWalReplicationContext.db
         && testWalReplicationContext.eState==STATE_WRITING ){
    rc = sqlite3_wal_replication_undo(
        testWalReplicationContext.db,
        testWalReplicationContext.zSchema
    );
  }
  if( rc==SQLITE_OK ){
    testWalReplicationContext.eState = STATE_UNDONE;
  }
  return rc;
}

/*
** A version of sqlite3_wal_replication.xEnd() that transitions the global
** replication context state to STATE_IDLE.
*/
static int testWalReplicationEnd(
  sqlite3_wal_replication *pReplication, void *pArg
){
  int rc = SQLITE_OK;
  assert( pArg==&testWalReplicationContext );
  assert( testWalReplicationContext.eState==STATE_PENDING
       || testWalReplicationContext.eState==STATE_COMMITTED
       || testWalReplicationContext.eState==STATE_UNDONE
  );
  testWalReplicationContext.eState = STATE_IDLE;
  if( testWalReplicationContext.eFailing==FAILING_END
   && testWalReplicationContext.iFailures>0
  ){
    rc = testWalReplicationContext.rc;
    testWalReplicationContext.iFailures--;
  }
  return rc;
}

/*
** This function returns a pointer to the WAL replication implemented in this
** file.
*/
sqlite3_wal_replication *testWalReplication(void){
  static sqlite3_wal_replication replication = {
    1,
    0,
    "test",
    0,
    testWalReplicationBegin,
    testWalReplicationAbort,
    testWalReplicationFrames,
    testWalReplicationUndo,
    testWalReplicationEnd,
  };
  return &replication;
}

/*
** This function returns a pointer to the WAL replication implemented in this
** file, but using a different registration name than testWalRepl.
**
** It's used to exercise the WAL replication registration APIs.
*/
sqlite3_wal_replication *testWalReplicationAlt(void){
  static sqlite3_wal_replication replication = {
    1,
    0,
    "test-alt",
    0,
    testWalReplicationBegin,
    testWalReplicationAbort,
    testWalReplicationFrames,
    testWalReplicationUndo,
    testWalReplicationEnd,
  };
  return &replication;
}

int main(){
  sqlite3* db_test1;
  sqlite3* db_test2;
  sqlite3_open("test1.db",&db_test1);
  sqlite3_open("test2.db",&db_test2);
  sqlite3_exec(db_test1,"pragma journal_mode=wal;",0,0,0);
  sqlite3_exec(db_test2,"pragma journal_mode=wal;",0,0,0);
  sqlite3_close(db_test2);
  sqlite3_close(db_test1);
}