#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../include/raft.h"
#include "../include/raft/uv.h"

#define MAX_PEERS 128  /* Max number of peers in a cluster */
#define APPLY_RATE 125 /* Apply a new entry every 125 milliseconds */
#define ADDR_LEN 64    /* The (maximum) length of an IP:PORT pair */

#define Log(SERVER_ID, FORMAT) printf("%d: " FORMAT "\n", SERVER_ID)
#define Logf(SERVER_ID, FORMAT, ...) \
    printf("%d: " FORMAT "\n", SERVER_ID, __VA_ARGS__)

/********************************************************************
 *
 * Sample application FSM that maintains a read-write register
 *
 ********************************************************************/

enum fsm_op_type {OP_READ, OP_WRITE};
struct fsm_op {
    enum fsm_op_type type;
    uint64_t value;
};

struct raft_buffer CreateRequest(enum fsm_op_type type, uint64_t val)
{
    struct raft_buffer buf;
    buf.len = sizeof(struct fsm_op);
    buf.base = raft_malloc(buf.len);
    if (buf.base == NULL) {
        return buf;
    }
    struct fsm_op* op = buf.base;
    op->type = type;
    op->value = val;
    return buf;
}

struct raft_buffer CreateRandomRequest(void)
{
    enum fsm_op_type type = random() % 2;
    uint64_t val = ((uint64_t) random()) % 100;
    return CreateRequest(type, val);
}

struct Fsm
{
    unsigned long long reg;
};

static int FsmApply(struct raft_fsm *fsm,
                    const struct raft_buffer *buf,
                    void **result)
{
    struct Fsm *f = fsm->data;
    if (buf->len != sizeof(struct fsm_op)) {
        fprintf(stderr, "Malformed: expected length %ld, got %ld\n", sizeof(struct fsm_op), buf->len);
        return RAFT_MALFORMED;
    }
    struct fsm_op* op = buf->base;
    switch(op->type) {
        case OP_WRITE:
            f->reg = op->value;
            *result = &f->reg;
            break;
        case OP_READ:
            *result = &f->reg;
            break;
    }
    if (op->type == OP_READ) {
        fprintf(stderr, "R: %lld\n", f->reg);
    } else {
        fprintf(stderr, "W -> %lld\n", f->reg);
    }
    return 0;
}

static int FsmSnapshot(struct raft_fsm *fsm,
                       struct raft_buffer *bufs[],
                       unsigned *n_bufs)
{
    struct Fsm *f = fsm->data;
    *n_bufs = 1;
    *bufs = raft_malloc(sizeof **bufs);
    if (*bufs == NULL) {
        return RAFT_NOMEM;
    }
    (*bufs)[0].len = sizeof(uint64_t);
    (*bufs)[0].base = raft_malloc((*bufs)[0].len);
    if ((*bufs)[0].base == NULL) {
        return RAFT_NOMEM;
    }
    *(uint64_t *)(*bufs)[0].base = f->reg;
    printf("Snapshotted value: %lld\n", f->reg);
    return 0;
}

static int FsmRestore(struct raft_fsm *fsm, struct raft_buffer *buf)
{
    struct Fsm *f = fsm->data;
    if (buf->len != sizeof(uint64_t)) {
        fprintf(stderr, "Malformed during attempted restore.\n");
        return RAFT_MALFORMED;
    }
    f->reg = *(uint64_t *)buf->base;
    raft_free(buf->base);
    fprintf(stderr, "Restored value: %lld\n", f->reg);
    return 0;
}

static int FsmInit(struct raft_fsm *fsm)
{
    struct Fsm *f = raft_malloc(sizeof *f);
    if (f == NULL) {
        return RAFT_NOMEM;
    }
    f->reg = 0;
    fsm->version = 1;
    fsm->data = f;
    fsm->apply = FsmApply;
    fsm->snapshot = FsmSnapshot;
    fsm->restore = FsmRestore;
    return 0;
}

static void FsmClose(struct raft_fsm *f)
{
    if (f->data != NULL) {
        raft_free(f->data);
    }
}

/********************************************************************
 *
 * Example struct holding a single raft server instance and all its
 * dependencies.
 *
 ********************************************************************/

struct Server;
typedef void (*ServerCloseCb)(struct Server *server);

struct Server
{
    void *data;                         /* User data context. */
    struct uv_loop_s *loop;             /* UV loop. */
    struct uv_timer_s timer;            /* To periodically apply a new entry. */
    const char *dir;                    /* Data dir of UV I/O backend. */
    struct raft_uv_transport transport; /* UV I/O backend transport. */
    struct raft_io io;                  /* UV I/O backend. */
    struct raft_fsm fsm;                /* Sample application FSM. */
    unsigned id;                        /* Raft instance ID. */
    char address[64];                   /* Raft instance address. */
    struct raft raft;                   /* Raft instance. */
    struct raft_transfer transfer;      /* Transfer leadership request. */
    ServerCloseCb close_cb;             /* Optional close callback. */
    uv_tcp_t req_server;                /* TCP server for client requests. */
};

static void serverRaftCloseCb(struct raft *raft)
{
    struct Server *s = raft->data;
    raft_uv_close(&s->io);
    raft_uv_tcp_close(&s->transport);
    FsmClose(&s->fsm);
    if (s->close_cb != NULL) {
        s->close_cb(s);
    }
}

static void serverTransferCb(struct raft_transfer *req)
{
    struct Server *s = req->data;
    raft_id id;
    const char *address;
    raft_leader(&s->raft, &id, &address);
    raft_close(&s->raft, serverRaftCloseCb);
}

/* Final callback in the shutdown sequence, invoked after the timer handle has
 * been closed. */
static void serverTimerCloseCb(struct uv_handle_s *handle)
{
    struct Server *s = handle->data;
    if (s->raft.data != NULL) {
        if (s->raft.state == RAFT_LEADER) {
            int rv;
            rv = raft_transfer(&s->raft, &s->transfer, 0, serverTransferCb);
            if (rv == 0) {
                return;
            }
        }
        raft_close(&s->raft, serverRaftCloseCb);
    }
}

/* Initialize the example server struct, without starting it yet. */
static int ServerInit(struct Server *s,
                      struct uv_loop_s *loop,
                      const char *dir,
                      unsigned id,
                      const char *bind,
                      char *nodes[],
                      unsigned num_nodes)
{
    struct raft_configuration configuration;
    struct timespec now;
    unsigned i;
    int rv;

    memset(s, 0, sizeof *s);

    /* Seed the random generator */
    timespec_get(&now, TIME_UTC);
    Logf(s->id, "Initializing random number generator (RNG) with seed: %u", (unsigned)(now.tv_nsec ^ now.tv_sec));
    srandom((unsigned)(now.tv_nsec ^ now.tv_sec));

    s->loop = loop;

    /* Add a timer to periodically try to propose a new entry. */
    rv = uv_timer_init(s->loop, &s->timer);
    if (rv != 0) {
        Logf(s->id, "uv_timer_init(): %s", uv_strerror(rv));
        goto err;
    }
    s->timer.data = s;

    /* Initialize the TCP-based RPC transport. */
    rv = raft_uv_tcp_init(&s->transport, s->loop);
    if (rv != 0) {
        goto err;
    }

    /* Initialize the libuv-based I/O backend. */
    rv = raft_uv_init(&s->io, s->loop, dir, &s->transport);
    if (rv != 0) {
        Logf(s->id, "raft_uv_init(): %s", s->io.errmsg);
        goto err_after_uv_tcp_init;
    }

    /* Initialize the finite state machine. */
    rv = FsmInit(&s->fsm);
    if (rv != 0) {
        Logf(s->id, "FsmInit(): %s", raft_strerror(rv));
        goto err_after_uv_init;
    }

    /* Save the server ID. */
    s->id = id;

    /* Render the address. */
    // sprintf(s->address, "172.22.0.%d:900%d", id + 1, id);
    const unsigned ADDR_SIZE = 64;
    strncpy(s->address, bind, ADDR_SIZE - 1);
    printf("Starting on %s\n", s->address);

    /* Initialize and start the engine, using the libuv-based I/O backend. */
    rv = raft_init(&s->raft, &s->io, &s->fsm, id, s->address);
    if (rv != 0) {
        Logf(s->id, "raft_init(): %s", raft_errmsg(&s->raft));
        goto err_after_fsm_init;
    }
    s->raft.data = s;

    /* Bootstrap the initial configuration if needed. */
    raft_configuration_init(&configuration);
    for (i = 0; i < num_nodes; i++) {
        char address[64];
        unsigned server_id = i + 1;
        strncpy(address, nodes[i], ADDR_SIZE - 1);
        printf("  Other node on %s\n", address);
        rv = raft_configuration_add(&configuration, server_id, address,
                                    RAFT_VOTER);
        if (rv != 0) {
            Logf(s->id, "raft_configuration_add(): %s", raft_strerror(rv));
            goto err_after_configuration_init;
        }
    }
    rv = raft_bootstrap(&s->raft, &configuration);
    if (rv != 0 && rv != RAFT_CANTBOOTSTRAP) {
        goto err_after_configuration_init;
    }
    raft_configuration_close(&configuration);

    raft_set_snapshot_threshold(&s->raft, 64);
    raft_set_snapshot_trailing(&s->raft, 16);
    raft_set_pre_vote(&s->raft, true);

    s->transfer.data = s;

    return 0;

err_after_configuration_init:
    raft_configuration_close(&configuration);
err_after_fsm_init:
    FsmClose(&s->fsm);
err_after_uv_init:
    raft_uv_close(&s->io);
err_after_uv_tcp_init:
    raft_uv_tcp_close(&s->transport);
err:
    return rv;
}

/* Called after a request to apply a new command to the FSM has been
 * completed. */
static void serverApplyCb(struct raft_apply *req, int status, void *result)
{
    struct Server *s = req->data;
    uint64_t res;
    raft_free(req);
    if (status != 0) {
        if (status != RAFT_LEADERSHIPLOST) {
            Logf(s->id, "raft_apply() callback: %s (%d)", raft_errmsg(&s->raft),
                 status);
        }
        return;
    }
    res = *(uint64_t *)result;
}

/* Called periodically every APPLY_RATE milliseconds. */
static void serverTimerCb(uv_timer_t *timer)
{
    struct Server *s = timer->data;
    struct raft_apply *req;
    int rv;

    if (s->raft.state != RAFT_LEADER) {
        return;
    }
    struct raft_buffer buf = CreateRandomRequest();
    if (buf.base == NULL) {
        Log(s->id, "serverTimerCb(): out of memory");
        return;
    }

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        Log(s->id, "serverTimerCb(): out of memory");
        return;
    }
    req->data = s;
    rv = raft_apply(&s->raft, req, &buf, 1, serverApplyCb);
    if (rv != 0) {
        Logf(s->id, "raft_apply(): %s", raft_errmsg(&s->raft));
        return;
    }
}

/* Start the example server. */
static int ServerStart(struct Server *s)
{
    int rv;

    Log(s->id, "starting");

    rv = raft_start(&s->raft);
    if (rv != 0) {
        Logf(s->id, "raft_start(): %s", raft_errmsg(&s->raft));
        goto err;
    }
    // Artificially create requests on a timer
    // rv = uv_timer_start(&s->timer, serverTimerCb, 0, APPLY_RATE);
    // if (rv != 0) {
    //     Logf(s->id, "uv_timer_start(): %s", uv_strerror(rv));
    //     goto err;
    // }

    return 0;

err:
    return rv;
}

/* Release all resources used by the example server. */
static void ServerClose(struct Server *s, ServerCloseCb cb)
{
    s->close_cb = cb;

    Log(s->id, "stopping");

    /* Close the timer asynchronously if it was successfully
     * initialized. Otherwise invoke the callback immediately. */
    if (s->timer.data != NULL) {
        uv_close((struct uv_handle_s *)&s->timer, serverTimerCloseCb);
    } else {
        s->close_cb(s);
    }
}

/********************************************************************
 *
 * Top-level main loop.
 *
 ********************************************************************/
struct uv_loop_s loop;

static void mainServerCloseCb(struct Server *server)
{
    struct uv_signal_s *sigint = server->data;
    uv_close((struct uv_handle_s *)sigint, NULL);
}

/* Handler triggered by SIGINT. It will initiate the shutdown sequence. */
static void mainSigintCb(struct uv_signal_s *handle, int signum)
{
    struct Server *server = handle->data;
    assert(signum == SIGINT);
    uv_signal_stop(handle);
    server->data = handle;
    ServerClose(server, mainServerCloseCb);
}

/********************************************************************
 * Handle requests from clients over a TCP connection.
 *******************************************************************/
typedef struct {
    void *data;
    uv_write_t req;
    uv_buf_t buf;
} write_req_t;

typedef struct {
    void *data;
    uv_buf_t buf;
    uv_stream_t *client;
} admin_command_t;

// Used to keep track of whom to respond to for a particular request
typedef struct {
    struct Server *server;
    uv_stream_t *client;
} client_request_ident;

typedef struct {
    void *data;
    raft_id serv_id;
    char serv_address[ADDR_LEN];
} add_server_t;

void alloc_buffer(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf);
void on_new_connection(uv_stream_t *server, int status);
void read_cb(uv_stream_t *client, ssize_t nread, const uv_buf_t *buf);
void respond_to_request(struct raft_apply *req, int status, void *result);
void after_reply(uv_write_t* req, int status);
void after_reply_to_administrative_command(uv_write_t *req, int status);

void alloc_buffer(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
    buf->base = (char*)malloc(suggested_size);
    buf->len = suggested_size;
}

void on_new_connection(uv_stream_t *server, int status) {
    struct Server *s = server->data;
    if (status < 0) {
        fprintf(stderr, "New connection error %s\n", uv_strerror(status));
        return;
    }
    uv_tcp_t *client = (uv_tcp_t *) malloc(sizeof(uv_tcp_t));
    client->data = s;
    uv_tcp_init(&loop, client);
    if (uv_accept(server, (uv_stream_t *) client) == 0) {
        uv_read_start((uv_stream_t *) client, alloc_buffer, read_cb);
    }
    else {
        uv_close((uv_handle_t *) client, NULL);
    }
}

void reply_with_leader_address(struct Server *s, uv_stream_t *client) {
    raft_id id;
    const char *leader_address;
    raft_leader(&s->raft, &id, &leader_address);
    if (leader_address == NULL) {
        leader_address = "";
    }
    char *reply = malloc(strlen(leader_address) + 5);
    sprintf(reply, "L %s\n", leader_address);

    // identify client for after_reply
    client_request_ident *cri = raft_malloc(sizeof *cri);
    cri->server = s;
    cri->client = client;

    // respond with result
    write_req_t* rreq = (write_req_t*)malloc(sizeof(write_req_t));
    rreq->buf = uv_buf_init(reply, strlen(reply));
    rreq->data = cri;
    uv_write((uv_write_t*) rreq, client, &rreq->buf, 1, after_reply);

}

void promote_server_to_voter_after_join(struct raft_change *req, int status) {
    add_server_t *as = req->data;
    struct Server *s = as->data;

    fprintf(stderr, "[Promote] req->cb = %p", (void*)(size_t) req->cb);
    int rv = raft_assign(&s->raft, req, as->serv_id, RAFT_VOTER, NULL);
    fprintf(stderr, "[Promote after assign] req->cb = %p", (void*)(size_t) req->cb);
    fprintf(stderr, "[Add CB status: %d] Trying to make server ID %llu (%s) a voter -> RET %s\n", status, as->serv_id, as->serv_address, rv == 0 ? "OK" : raft_strerror(rv));
    // TODO: free memory
}

void after_removal(struct raft_change *req, int status) {
    add_server_t *as = req->data;
    struct Server *s = as->data;
    fprintf(stderr, "[In removal callback] req->cb = %p", (void*)(size_t) req->cb);
    fprintf(stderr, "Removed server ID %llu -> RET %d\n", as->serv_id, status);
}

char *configuration_to_string(struct raft_configuration *c) {
    unsigned n = c->n;
    char *str = raft_malloc(ADDR_LEN * (n + 2));
    unsigned num_chars = 0;

    for (unsigned i = 0 ; i < n; i++) {
        struct raft_server *s = &(c->servers[i]);
        num_chars += sprintf(str + num_chars, "%d -> %s ", s->id, s->address);
    }
    return str;
}

void read_cb(uv_stream_t* client, ssize_t nread, const uv_buf_t* buf) {
    // Compare with serverTimerCb
    // check if Raft leader
    struct Server *s = client->data;
    char *cmd = buf->base;

    // If we are not the leader, we can only serve C = Configuration requests
    // Show we're not the leader by immediately closing the connection
    bool can_service_request = s->raft.state == RAFT_LEADER || cmd[0] == 'C';
    if (!can_service_request) {
        reply_with_leader_address(s, client);
        // uv_close((uv_handle_t*)client, NULL);
        free(buf->base);
        return;
    }

    // Logf(s->id, "Received request: %s", cmd);
    if (nread > 0) {
        // Specially handle configuration change requests
        // A = Add [id] [ip:port]
        // R = Remove [id]
        // C = (Print) Configuration
        if (cmd[0] == 'A' || cmd[0] == 'D' || cmd[0] == 'C') {
            // fprintf(stderr, "Received membership request: %s", cmd);
            int rv = 0;
            raft_id serv_id;
            char *reply = NULL;

            if (cmd[0] == 'A') {
                char serv_address[ADDR_LEN];
                sscanf(cmd, "A %llu %s", &serv_id, &serv_address);

                add_server_t *as = raft_malloc(sizeof *as);
                as->data = s;
                as->serv_id = serv_id;
                strncpy(as->serv_address, serv_address, ADDR_LEN);

                struct raft_change *rc = raft_malloc(sizeof *rc);
                rc->data = as;
                rc->cb = NULL;
                // By default, nodes are given the RAFT_STANDY log --> they replicate the log, but do not vote
                // after the add completes, promote_server is called (callback) to make it a voter
                fprintf(stderr, "[Before add] rc->cb = %p", (void*)(size_t) rc->cb);
                rv = raft_add(&s->raft, rc, serv_id, serv_address, promote_server_to_voter_after_join);
                fprintf(stderr, "[After add] rc->cb = %p", (void*)(size_t) rc->cb);
                fprintf(stderr, "Trying to add server with ID %llu at %s -> RET %s\n", serv_id, serv_address, rv == 0 ? "OK" : raft_strerror(rv));
            } else if (cmd[0] == 'D') {
                sscanf(cmd, "D %llu", &serv_id);
                add_server_t *as = raft_malloc(sizeof *as);
                as->data = s;
                as->serv_id = serv_id;
                strncpy(as->serv_address, "", ADDR_LEN);

                struct raft_change *rc = raft_malloc(sizeof *rc);
                rc->data = as;
                rc->cb = NULL;
                fprintf(stderr, "[Before remove] rc->cb = %p", (void*)(size_t) rc->cb);
                rv = raft_remove(&s->raft, rc, serv_id, after_removal);
                fprintf(stderr, "[After remove] rc->cb = %p", (void*)(size_t) rc->cb);
                fprintf(stderr, "Trying to remove server with ID %llu -> RET %s\n", serv_id, rv == 0 ? "OK" : raft_strerror(rv));
            } else { // (cmd[0] == 'C')
                // we get a copy in case it changes while we execute (?)
                struct raft_configuration rc = s->raft.configuration;
                // reserve enough space for the reply
                unsigned chars_needed = 0;
                for (unsigned i = 0; i < rc.n; i++) {
                    // hack to get the number of characters in ID; per https://stackoverflow.com/a/7200873
                    unsigned ndigits = snprintf(NULL, 0, "%llu", rc.servers[i].id);
                    chars_needed += ndigits;
                    chars_needed += strlen(rc.servers[i].address);
                    chars_needed += 6; // spacing: ID -> ADDR
                }
                reply = raft_malloc(chars_needed);
                int chars_written = 0;
                raft_id leader_id;
                const char *leader_address;
                raft_leader(&s->raft, &leader_id, &leader_address);
                // iterate over servers in the configuration
                for (unsigned i = 0; i < rc.n; i++) {
                    struct raft_server *serv = &rc.servers[i];
                    char replica_type = (serv->id == leader_id) ? 'L' : 'F'; // leader or follower
                    chars_written += sprintf(reply + chars_written, "%llu %s %c;", serv->id, serv->address, replica_type);
                }
                fprintf(stderr, "%s\n", reply);
            }
            // respond with configuration or status message
            if (reply == NULL) {
                reply = rv == 0 ? strdup("OK") : strdup(raft_strerror(rv));
            }
            admin_command_t *ac = raft_malloc(sizeof(admin_command_t));
            uv_write_t *rp = raft_malloc(sizeof(uv_write_t));
            ac->data = ac;
            ac->buf = uv_buf_init(reply, strlen(reply));;
            ac->client = client;
            rp->data = ac;
            uv_write((uv_write_t*) rp, client, &ac->buf, 1, after_reply_to_administrative_command);

            // char *str_ret = rv == 0 ? "OK" : raft_strerror(rv);
            // Close the connection to the client after scheduling the configuration change
            // uv_close((uv_handle_t*)client, NULL);
        } else {
            // Only accept R or W [int] requests
            if (!(cmd[0] == 'R' || cmd[0] == 'W')) {
                Logf(s->id, "Invalid request: %s", cmd);
                uv_close((uv_handle_t*)client, NULL);
                return;
            }

            // fprintf(stderr, "Received KV request: %s", cmd);
            // Parse request
            enum fsm_op_type t = cmd[0] == 'R' ? OP_READ : OP_WRITE;
            uint64_t val = 0;
            if (t == OP_WRITE) {
                val = (uint64_t) atoi(&cmd[1]);
            }
            // TODO: add OOM checks
            struct raft_buffer rb = CreateRequest(t, val); // FIXME: I suppose this gets freed by raft_apply?
            // Logf(s->id, "Parsed request: %c %lu", cmd[0], val);
            struct raft_apply *creq = raft_malloc(sizeof *creq);
            client_request_ident *cri = raft_malloc(sizeof *cri);
            cri->server = s;
            cri->client = client;
            creq->data = cri;

            int rv = raft_apply(&s->raft, creq, &rb, 1, respond_to_request);
            if (rv != 0) {
                Logf(s->id, "raft_apply(): %s", raft_errmsg(&s->raft));
                return;
            }
        }
    }
    if (nread < 0) {
        if (nread != UV_EOF) {
            fprintf(stderr, "Read error %s\n", uv_err_name(nread));
        }
        uv_close((uv_handle_t*)client, NULL);
    }
    free(buf->base);
}

void respond_to_request(struct raft_apply *req, int status, void *result) {
    client_request_ident *cri = req->data;
    struct Server *s = cri->server;

    raft_free(req);
    if (status != 0) {
        if (status != RAFT_LEADERSHIPLOST) {
            Logf(s->id, "raft_apply() callback: %s (%d)", raft_errmsg(&s->raft),
                 status);
        }
        return;
    }
    uint64_t res = *(uint64_t *)result;
    const size_t max_resp_sz = 64;
    char *res_str = raft_malloc(max_resp_sz);
    int res_len = snprintf(res_str, max_resp_sz, "%ld\n", res);
    // respond with result
    write_req_t* rreq = (write_req_t*)malloc(sizeof(write_req_t));
    rreq->buf = uv_buf_init(res_str, res_len);
    rreq->data = cri;
    uv_write((uv_write_t*) rreq, cri->client, &rreq->buf, 1, after_reply);

    // raft_free(cri);
}

void after_reply_to_administrative_command(uv_write_t *req, int status) {
    // Close the connection
    admin_command_t *ac = req->data;
    uv_close((uv_handle_t*) ac->client, NULL);
    // Free resources
    free(ac->buf.base);
    free(ac);
    free(req);
}

void after_reply(uv_write_t* req, int status) {
    if (status) {
        fprintf(stderr, "Write error %s\n", uv_strerror(status));
    }
    write_req_t* wr = (write_req_t*)req;
    // Close the connection
    client_request_ident *cri = wr->data;
    uv_handle_t *client = (uv_handle_t *) cri->client;
    uv_close((uv_handle_t*)client, NULL);
    // Free resources
    free(wr->buf.base);
    free(wr);
    free(cri);
}

int main(int argc, char *argv[])
{
    setbuf(stdout, NULL);
    struct uv_signal_s sigint; /* To catch SIGINT and exit. */
    struct Server server;

    const char *dir = NULL;
    unsigned id = 0;
    const char *bind = NULL;
    unsigned client_port = 0;
    unsigned num_nodes = 0;
    char *peers[MAX_PEERS];
    int rv;

    int opt;
    /*
        -d <dir>
        -i <id>
        -b <addr:port> to bind on
        -n <addr:port> of node in the cluster (can be repeated; must include current node)
     */
    while ((opt = getopt(argc, argv, "d:i:b:p:n:")) != -1) {
        switch(opt) {
            case 'd':
                dir = strdup(optarg);
                break;
            case 'i':
                id = (unsigned) atoi(optarg);
                break;
            case 'b':
                bind = strdup(optarg);
                break;
            case 'p':
                client_port = (unsigned) atoi(optarg);
                break;
            case 'n':
                peers[num_nodes++] = strdup(optarg);
                break;
            case '?':
                printf("Unknown option: %c\n", optopt);
                break;
            case ':':
                printf("Missing arg for %c\n", optopt);
                break;
        }
    }
    if (dir == NULL || id == 0 || bind == NULL || client_port == 0 || num_nodes == 0) {
        printf("Usage: %s -d <dir> -i <id> -b <addr:port> -p <port> -n <addr:port>\n",
               argv[0]);
        return 1;
    }
    Log(id, "Entry");

    /* Ignore SIGPIPE, see https://github.com/joyent/libuv/issues/1254 */
    signal(SIGPIPE, SIG_IGN);

    /* Initialize the libuv loop. */
    rv = uv_loop_init(&loop);
    if (rv != 0) {
        Logf(id, "uv_loop_init(): %s", uv_strerror(rv));
        goto err;
    }

    /* Initialize the example server. */
    rv = ServerInit(&server, &loop, dir, id, bind, peers, num_nodes);
    if (rv != 0) {
        goto err_after_server_init;
    }

    /* Add a signal handler to stop the example server upon SIGINT. */
    rv = uv_signal_init(&loop, &sigint);
    if (rv != 0) {
        Logf(id, "uv_signal_init(): %s", uv_strerror(rv));
        goto err_after_server_init;
    }
    sigint.data = &server;
    rv = uv_signal_start(&sigint, mainSigintCb, SIGINT);
    if (rv != 0) {
        Logf(id, "uv_signal_start(): %s", uv_strerror(rv));
        goto err_after_signal_init;
    }

    /* Start the server. */
    rv = ServerStart(&server);
    if (rv != 0) {
        goto err_after_signal_init;
    }

    /* Start listening for client requests. */
    struct sockaddr_in req_addr;
    uv_ip4_addr("0.0.0.0", client_port, &req_addr);
    uv_tcp_init(&loop, &server.req_server);
    server.req_server.data = &server;
    uv_tcp_bind(&server.req_server, (const struct sockaddr*) &req_addr, 0);
    const int conn_backlog = 128;
    int r = uv_listen((uv_stream_t *) &server.req_server, conn_backlog, on_new_connection);
    if (r) {
        return fprintf(stderr, "Error on listening: %s.\n", 
                uv_strerror(r));
    }
    Logf(id, "Listening for client requests on port %d", client_port);

    /* Run the event loop until we receive SIGINT. */
    rv = uv_run(&loop, UV_RUN_DEFAULT);
    if (rv != 0) {
        Logf(id, "uv_run_start(): %s", uv_strerror(rv));
    }

    Log(id, "pre close reached");
    uv_loop_close(&loop);
    Log(id, "close reached");

    return rv;

err_after_signal_init:
    uv_close((struct uv_handle_s *)&sigint, NULL);
err_after_server_init:
    ServerClose(&server, NULL);
    uv_run(&loop, UV_RUN_DEFAULT);
    uv_loop_close(&loop);
err:
    return rv;
}
