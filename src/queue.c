#include "queue.h"
#include <stdio.h>
#include <errno.h>


queue_t *create_queue(void) {
    queue_t* queue = calloc(1, sizeof(queue_t));

    if (queue == NULL){
        errno = EINVAL;
        return NULL;
    }

    // if its not null, meaning we were able to allocate, then set fields
    queue -> front = NULL;
    queue -> rear = NULL;
    if (sem_init(&queue -> items, 0, 0) == -1){
        return NULL;
    }

    if (pthread_mutex_init(&queue -> lock, NULL) == -1){
        return NULL;
    }

    queue -> invalid = false;

    // return the queue
    return queue;
}



bool invalidate_queue(queue_t *self, item_destructor_f destroy_function) {
    if (self == NULL || destroy_function == NULL){
        errno = EINVAL;
        return false;
    }

    // else, the arguments are valid, so lets invalidate this queue properly
    // grab the mutex
    pthread_mutex_lock(&self -> lock);

    // if its already invalid, return false
    if (self -> invalid == true){
        errno = EINVAL;
        pthread_mutex_unlock(&self -> lock);
        return false;
    }

    // call destroy function on the item of the front
    queue_node_t *node = self -> front;

    while (node != NULL){
        queue_node_t *nextNode = node -> next;
        destroy_function(node -> item);
        free(node);

        node = nextNode;
    }

    self -> invalid = true;

    pthread_mutex_unlock(&self -> lock);
    return true;
}



bool enqueue(queue_t *self, void *item) {
    // begin by checking arguments
    if (self == NULL || item == NULL){
        errno = EINVAL;
        return false;
    }

    // args are valid, grab the mutex
    pthread_mutex_lock(&self -> lock);

    // check if the queue is invalid
    if (self -> invalid == true){
        errno = EINVAL;
        pthread_mutex_unlock(&self -> lock);
        return false;
    }

    // if its still valid, then add a new queue_node_t to it
    // calloc for space
    queue_node_t *node = calloc(1, sizeof(queue_node_t));

    // if calloc returned a NULL pointer, then set to false and exit
    if (node == NULL){
        errno = EINVAL;
        pthread_mutex_unlock(&self -> lock);
        return false;
    }

    // else, calloc did give space. set the item field to our current item
    node -> item = item;
    node -> next = NULL;

    // and now, add the item to the queue
    // if the front is null, put it directly at the front. else, go to the end
    if (self -> front == NULL){
        self -> front = node;
        self -> rear = node;
    }

    else{
        // set the next of the rear to this item
        self -> rear -> next = node;
        // set the queue rear to this item
        self -> rear = node;
    }

    // increment our item count by 1
    sem_post(&self -> items);

    // unlock and return
    pthread_mutex_unlock(&self -> lock);
    return true;
}

void *dequeue(queue_t *self) {
    // first check the arg
    if (self == NULL || self -> invalid == true){
        errno = EINVAL;
        return NULL;
    }

    // first try to grab an item
    sem_wait(&self -> items);
    // then grab the mutex
    pthread_mutex_lock(&self -> lock);
    // after grabbing both, check if the queue is still valid
    if (self -> invalid == true){
        errno = EINVAL;
        pthread_mutex_unlock(&self -> lock);
        return NULL;
    }

    // otherwise, the queue is still valid, so pop the first item off
    void* item = self -> front -> item;

    // grab a pointer to the next node
    queue_node_t *next = self -> front -> next;

    // free the current node
    free(self -> front);

    // set the front to the 'next' node that was obtained earlier
    self -> front = next;

    // and now unlock and return the item
    pthread_mutex_unlock(&self -> lock);
    return item;
}
