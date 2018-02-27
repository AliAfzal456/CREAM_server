#include "csapp.h"
#include "cream.h"
#include "utils.h"
#include "queue.h"


queue_t *queue;
hashmap_t *hashmap;


void destroy_func(map_key_t key, map_val_t val) {
    free(key.key_base);
    free(val.val_base);
}

void handleClear(int connfd){
    clear_map(hashmap);

    response_header_t *response = calloc(1, sizeof(response_header_t));
    response -> response_code = OK;
    response -> value_size = 0;
    write(connfd, response, sizeof(response));
    free(response);
}

void handleEvict(int connfd, int key_size){
    char *key_ptr = malloc(key_size);
    read (connfd, key_ptr, key_size);
    delete(hashmap, MAP_KEY(key_ptr, key_size));

    response_header_t *response = calloc(1, sizeof(response_header_t));
    response -> response_code = OK;
    response -> value_size = 0;
    write(connfd, response, sizeof(response));
    free(response);
}

void handleGet(int connfd, int key_size){
    char *key_ptr = malloc(key_size);
    read (connfd, key_ptr, key_size);
    map_val_t val = get(hashmap, MAP_KEY(key_ptr, key_size));

    response_header_t * response = calloc(1, sizeof(response_header_t));
    if (val.val_len == 0){
        response -> response_code = BAD_REQUEST;
        response -> value_size = 0;
        write(connfd, response, sizeof(response));
    }

    else{
        response -> response_code = OK;
        response -> value_size = val.val_len;
        write(connfd, response, sizeof(response));
        write(connfd, val.val_base, val.val_len);
    }
    free(response);
}

void handlePut(int connfd, int key_size, int value_size){
    // malloc for the key and value
    char *key_ptr = malloc(key_size);
    char *val_ptr = malloc(value_size);

    int read1 = read(connfd, key_ptr, key_size);
    int read2 = read(connfd, val_ptr, value_size);
    bool putted = put(hashmap, MAP_KEY(key_ptr, key_size), MAP_VAL(val_ptr, value_size), true);

    // the next key_size bytes are the key, so read them
    // send back a response after putting
    response_header_t *response = calloc(1, sizeof(response_header_t) + 1);
    if (putted == true){
        response -> response_code = OK;
        response -> value_size = 0;
    }

    //else, there was an error while putting
    else{
        response -> response_code = BAD_REQUEST;
        response -> value_size = 0;
    }

    // send the header
    int count = write(connfd, response, sizeof(response));
    free(response);

    // temp post, please ignore
    if (read1 <= 0 || read2 <= 0 || count <= 0){

    }
}

void handle_request(int connfd){
    // alocate space for header
    bool isInvalid = false;
    request_header_t *header = calloc(1, sizeof(request_header_t));
    int count = read(connfd, header, sizeof(request_header_t));

    if (count < 0){

    }
    // if we are putting
    if (header -> request_code == PUT){
        // first check the sizes of the key and val. if its too big, handle put
        if (header -> key_size >= MIN_KEY_SIZE && header -> key_size <= MAX_KEY_SIZE && header -> value_size >= MIN_VALUE_SIZE && header -> value_size <= MAX_VALUE_SIZE){
            handlePut(connfd, header -> key_size, header -> value_size);
        }
        else{
            isInvalid = true;
        }
    }

    else if (header -> request_code == GET){
        if (header -> key_size >= MIN_KEY_SIZE && header -> key_size <= MAX_KEY_SIZE){
            handleGet(connfd, header -> key_size);
        }
        else{
            isInvalid = true;
        }
    }

    else if (header -> request_code == EVICT){
        if (header -> key_size >= MIN_KEY_SIZE && header -> key_size <= MAX_KEY_SIZE){
            handleEvict(connfd, header -> key_size);
        }
        else{
            isInvalid = true;
        }
    }

    else if (header -> request_code == CLEAR){
        handleClear(connfd);
    }

    // else the header code is something weird, so we don't support it
    else{
        response_header_t *response = calloc(1, sizeof(response_header_t) + 1);
        response -> response_code = UNSUPPORTED;
        response -> value_size = 0;
    }

    // if it turns out that we matched a header, but the size wasn't valid, its a bad request
    if (isInvalid == true){
        response_header_t *response = calloc(1, sizeof(response_header_t) + 1);
        response -> response_code = BAD_REQUEST;
        response -> value_size = 0;
    }

    // free the allocated header
    free(header);
}

void* thread(void* vargp){
    while(1){
        void* ptr = dequeue(queue);
        int connfd = *(int*)ptr; //(*(int*)dequeue(queue));
        // do work here. Connection gets closed after client sends the word 'exit' or if there is some error
        handle_request(connfd);
        Close(connfd);
    }
}

int main(int argc, char *argv[]) {
    // first thing is to check if arg2 is -h
    if (argc < 2){
        // error
        exit(EXIT_FAILURE);
    }
    signal(SIGPIPE, SIG_IGN);
    // if we have at least 2 args, check if first arg is -h
    if (strcmp(argv[1], "-h") == 0){
        printf("%s\n", "./cream [-h] NUM_WORKERS PORT_NUMBER MAX_ENTRIES\n"
                       "-h                 Displays this help menu and returns EXIT_SUCCESS.\n"
                       "NUM_WORKERS        The number of worker threads used to service requests.\n"
                       "PORT_NUMBER        Port number to listen on for incoming connections.\n"
                       "MAX_ENTRIES        The maximum number of entries that can be stored in `cream`'s underlying data store.\n");

        exit(EXIT_SUCCESS);
    }

    // so the first arg wasn't -h. there should be exactly 4 args total then (prog name, num workers, port num, max entries)
    if (argc != 4){
        exit(EXIT_FAILURE);
    }

    int NUM_WORKERS = atoi(argv[1]);
    int MAX_ENTRIES = atoi(argv[3]);


    // using the 3 values above, validate them.
    if (NUM_WORKERS < 1 || MAX_ENTRIES < 1){
        exit(EXIT_FAILURE);
    }

    // after validation, lets start the skeleton of the server
    int i,listenfd, connfd;
    socklen_t clientlen;
    struct sockaddr_storage clientaddr;
    pthread_t tid;

    // create the listener
    listenfd = Open_listenfd(argv[2]);
    // create the queue
    queue = create_queue();
    hashmap = create_map(MAX_ENTRIES, jenkins_one_at_a_time_hash, destroy_func);

    for (i = 0; i < NUM_WORKERS; i++){
        Pthread_create(&tid, NULL, thread, NULL);
    }
    while(1){
        clientlen = sizeof(struct sockaddr_storage);
        connfd = Accept(listenfd, (SA *)&clientaddr, &clientlen);
        enqueue(queue, &connfd);
    }
    exit(0);
}



