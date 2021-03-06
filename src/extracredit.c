#include "utils.h"
#include <errno.h>
#include <stdio.h>
#include <string.h>

#include <time.h>
#include <unistd.h>
//#include "extracredit.h"


#define MAP_NODE2(key_arg, val_arg, tombstone_arg, use_arg) (map_node_t) {.key = key_arg, .val = val_arg, .tombstone = tombstone_arg, .use = use_arg}
hashmap_t *create_map(uint32_t capacity, hash_func_f hash_function, destructor_f destroy_function) {
    // if either of the args is null, return null
    if (hash_function == NULL || destroy_function == NULL || capacity < 0){
        errno = EINVAL;
        return NULL;
    }

    // else, both functions are valid, so we can move on to creating the struct
    hashmap_t *hashmap = calloc(1, sizeof(hashmap_t));

    // check if calloc returned correctly
    if (hashmap == NULL){
        errno = EINVAL;
        return NULL;
    }

    hashmap -> capacity = capacity;
    hashmap -> size = 0;
    hashmap -> hash_function = hash_function;
    hashmap -> destroy_function = destroy_function;
    hashmap -> num_readers = 0;
    hashmap -> counter = 0;
    hashmap -> invalid = false;

    if (pthread_mutex_init(&hashmap -> write_lock, NULL) == -1){
        return NULL;
    }

    if (pthread_mutex_init(&hashmap -> fields_lock, NULL) == -1){
        return NULL;
    }

    // after setting those fields, theres 1 field left. the node base address.
    // we need to calloc for the number of nodes and store the starting address into the hashmap
    map_node_t *baseNode = calloc(capacity, sizeof(map_node_t));

    // after calloc-ing for the space, check for errors
    if (baseNode == NULL){
        free(hashmap);
        return NULL;
    }
    // this is the base address of the nodes :eyes:
    hashmap -> nodes = baseNode;
    // return it.
    return hashmap;
}

bool put(hashmap_t *self, map_key_t key, map_val_t val, bool force) {
    if (self == NULL || key.key_base == NULL || val.val_base == NULL || key.key_len == 0 || val.val_len == 0){
        errno = EINVAL;
        return false;
    }

    // if none of them are null, we will need to put something into it.
    // grab the mutex to write

    pthread_mutex_lock(&self -> write_lock);

    // check if its been invalidated
    if (self -> invalid == true){
        errno = EINVAL;
        pthread_mutex_unlock(&self -> write_lock);
        return false;
    }

    // after we grab this lock, we know that no one else
    // is reading, and no one else is writing. do stuff
    // we want to put the key, val at some index x, so get the index
    int index = get_index(self, key);

    // if the key doesn't exist or it existed in the past (tombstone), then just put the new key and val and return
    if (self -> nodes[index].key.key_len == 0 || self -> nodes[index].tombstone == true){
        self -> nodes[index].key = key; // put key
        self -> nodes[index].val = val; // put val
        self -> size += 1;  // increment size by 1

        // NEW: update the amount of stuff (used for last-used time)
        self -> counter += 1;
        // NEW: update this nodes last -used time
        self -> nodes[index].use = self -> counter;

        self -> nodes[index].tombstone = false;
        // set the time for the start
        time (&(self -> nodes[index].start));

        // unlock after putting key and val
        pthread_mutex_unlock(&self -> write_lock);
        return true;
    }

    // else if its not empty and its not a tombstone, we check for replacement
    if (self -> nodes[index].key.key_len != 0){
        // check if the keys are the same. if they are, update the val
        if (self -> nodes[index].key.key_len == key.key_len){
            // if the memory at index key == arg key, then its the same key. update the val and return
            if (memcmp(self -> nodes[index].key.key_base, key.key_base, key.key_len) == 0){

                self -> nodes[index].val = val;
                self -> nodes[index].tombstone = false;
                // NEW: update the amount of stuff (used for last-used time)
                self -> counter += 1;
                // NEW: update this nodes last -used time
                self -> nodes[index].use = self -> counter;

                // set the time for the start
                time (&(self -> nodes[index].start));

                pthread_mutex_unlock(&self -> write_lock);
                return true;
            }
        }

        // else if the key lengths aren't the same, then probe until same key or tombstone is found
        int oldIndex = index;
        oldIndex += 1;
        int currIndex = 0;

        // as long as we have not looped all the way around, keep incrementing and checking index % capacity
        while ((oldIndex % self -> capacity) != index){
            currIndex = oldIndex % self -> capacity;

            if (self -> nodes[currIndex].key.key_len == 0 || self -> nodes[currIndex].tombstone == true){
                self -> nodes[currIndex].key = key;
                self -> nodes[currIndex].val = val;
                self -> nodes[currIndex].tombstone = false;
                self -> size += 1;

                // NEW: update the amount of stuff (used for last-used time)
                self -> counter += 1;
                // NEW: update this nodes last -used time
                self -> nodes[currIndex].use = self -> counter;

                // set the time for the start
                time (&(self -> nodes[currIndex].start));

                // unlock and return
                pthread_mutex_unlock(&self -> write_lock);
                return true;
            }

            oldIndex += 1;
        }
    }
    // so we looped all the way around, and there were no open slots or dead slots.
    // this means that we need to check the force parameter
    if (force == true){
        // find if a node has passed its alloted time (ttl)
        bool didPass = false;
        time_t end;
        time (&end);
        int diff = 0;

        int oldIndex = index;
        oldIndex += 1;
        int currIndex = 0;

        if (difftime(end, self -> nodes[index].start) >= TTL){
            // if current node is usable, then use it.
            didPass = true;
            // set free this node and set.
            self -> destroy_function(self -> nodes[index].key, self -> nodes[index].val);

            // just simply put it at the required index.
            self -> nodes[index].key = key;
            self -> nodes[index].val = val;

            self -> counter += 1;
            // NEW: update this nodes last -used time
            self -> nodes[index].use = self -> counter;

            self -> nodes[index].val = val;
            // reset the time
            time(&(self -> nodes[index].start));
        }

        else{
            // as long as we have not looped all the way around, keep incrementing and checking index % capacity
            while ((oldIndex % self -> capacity) != index && didPass == false){
                currIndex = oldIndex % self -> capacity;
                if (self -> nodes[currIndex].key.key_len != 0){
                    // check if this current - occupied is done
                    diff = difftime(end, self -> nodes[currIndex].start);

                    if (diff >= TTL){
                        didPass = true;
                        // set free this node and set.
                        self -> destroy_function(self -> nodes[currIndex].key, self -> nodes[currIndex].val);

                        // just simply put it at the required index.
                        self -> nodes[currIndex].key = key;
                        self -> nodes[currIndex].val = val;

                        self -> counter += 1;
                        // NEW: update this nodes last -used time
                        self -> nodes[currIndex].use = self -> counter;

                        self -> nodes[currIndex].val = val;
                        // reset the time
                        time(&(self -> nodes[currIndex].start));
                    }
                }
                oldIndex += 1;
            }
        }
        // if no ttl, find the last - used node
        if (didPass == false){
            int lastUsed = -1;
            int index = 0;
            for (int i = 0; i < self -> capacity; i ++){
                int currUse = self -> nodes[i].use;

                if (lastUsed == -1 || currUse < lastUsed){
                    lastUsed = currUse;
                    index = i;
                }
            }
            // curruse has the last used index. destroy it and put
            // destory the old node
            self -> destroy_function(self -> nodes[index].key, self -> nodes[index].val);

            // just simply put it at the required index.
            self -> nodes[index].key = key;
            self -> nodes[index].val = val;

            self -> counter += 1;
            // NEW: update this nodes last -used time
            self -> nodes[index].use = self -> counter;

            time(&(self -> nodes[index].start));
        }
        pthread_mutex_unlock(&self -> write_lock);
        return true;
    }

    // if we are not forcing, and the map is full, set errno to enomem
    errno = ENOMEM;
    pthread_mutex_unlock(&self -> write_lock);
    return false;
}

map_val_t get(hashmap_t *self, map_key_t key) {
    if (self == NULL || key.key_base == NULL ||  key.key_len == 0){
        errno = EINVAL;
        return MAP_VAL(NULL, 0);
    }

    // if none of them are null, we are reading. first grab the mutex to increase num readers
    pthread_mutex_lock(&self -> fields_lock);
    self -> num_readers += 1;
    if (self -> num_readers == 1){
        pthread_mutex_lock(&self -> write_lock);
    }
    pthread_mutex_unlock(&self -> fields_lock);

    // now we have complete control over reading
    // use the key to calculate the index for which we have to read
    int index = get_index(self, key);
    int oldIndex = index + 1;
    int currIndex = 0;
    void* returnAddy = NULL;
    int len = 0;

    if (self -> invalid == false){

        if (self -> nodes[index].key.key_len == key.key_len){
            if (memcmp(self -> nodes[index].key.key_base, key.key_base, key.key_len) == 0  &&  self -> nodes[index].tombstone == false){
                // if they're the same key, store the variable

                time_t end;
                time (&end);
                int diff = 0;
                diff = difftime(end, self -> nodes[index].start);

                if (diff < TTL){
                    returnAddy = self -> nodes[index].val.val_base;
                    len = self -> nodes[index].val.val_len;

                    // was just accessed, so its access time increases as well
                    // NEW: update the amount of stuff (used for last-used time)
                    self -> counter += 1;
                    // NEW: update this nodes last -used time
                    self -> nodes[index].use = self -> counter;
                }

                else{
                    // ttl happened, so free this node and continue like nothing happened
                    self -> destroy_function(self -> nodes[index].key, self -> nodes[index].val);
                    // NEW: update this nodes last -used time
                    self -> nodes[index].use = 0;
                    self -> nodes[index].tombstone = true;
                }
            }
        }

        while ((oldIndex % (self -> capacity)) != index){
            currIndex = oldIndex % self -> capacity;
            if (self -> nodes[currIndex].key.key_len == key.key_len){
                if (memcmp(self -> nodes[currIndex].key.key_base, key.key_base, key.key_len) == 0 && self -> nodes[currIndex].tombstone == false){
                    time_t end;
                    time (&end);
                    int diff = 0;
                    diff = difftime(end, self -> nodes[currIndex].start);

                    if (diff < TTL){

                        returnAddy = self -> nodes[currIndex].val.val_base;
                        len = self -> nodes[currIndex].val.val_len;

                        // NEW: update the amount of stuff (used for last-used time)
                        self -> counter += 1;
                        // NEW: update this nodes last -used time
                        self -> nodes[currIndex].use = self -> counter;
                    }

                    else{
                        self -> destroy_function(self -> nodes[currIndex].key, self -> nodes[currIndex].val);
                        self -> nodes[currIndex].use = 0;
                        self -> nodes[currIndex].tombstone = true;
                    }
                }
            }
            oldIndex += 1;
        }
    }

    // at this point, the value is either null, 0 or it was set in one of the loops. unlock the stuff and return
    // after all is read, we decrease num readers
    pthread_mutex_lock(&self -> fields_lock);
    self -> num_readers -= 1;
    if (self -> num_readers == 0)
        pthread_mutex_unlock(&self -> write_lock);
    pthread_mutex_unlock(&self -> fields_lock);

    if (returnAddy == NULL)
        errno = EINVAL;
    return MAP_VAL(returnAddy, len);
}

map_node_t delete(hashmap_t *self, map_key_t key) {
    if (self == NULL || key.key_len == 0 || key.key_base == NULL){
        errno = EINVAL;
        return MAP_NODE(MAP_KEY(NULL, 0), MAP_VAL(NULL, 0), false);
    }

    // self is valid, key is valid, grab the mutex
    pthread_mutex_lock(&self -> write_lock);

    // check if the map is invalid
    if (self -> invalid == true){
        errno = EINVAL;
        pthread_mutex_unlock(&self -> write_lock);
        return MAP_NODE(MAP_KEY(NULL, 0), MAP_VAL(NULL, 0), false);
    }

    int index = get_index(self, key);
    int oldIndex = index + 1;
    int currIndex = 0;

    if (self -> invalid == false){

        if (self -> nodes[index].key.key_len == key.key_len){
            if (memcmp(self -> nodes[index].key.key_base, key.key_base, key.key_len) == 0  &&  self -> nodes[index].tombstone == false){
                // if they're the same key, store the variable
                self -> nodes[index].tombstone = true;
                self -> nodes[index].use = 0;
                self -> size -= 1;
                map_node_t returnVal = self -> nodes[index];
                // unlock
                pthread_mutex_unlock(&self -> write_lock);
                return returnVal;
            }
        }

        while ((oldIndex % (self -> capacity)) != index){
            currIndex = oldIndex % self -> capacity;
            if (self -> nodes[currIndex].key.key_len == key.key_len){
                if (memcmp(self -> nodes[currIndex].key.key_base, key.key_base, key.key_len) == 0 && self -> nodes[currIndex].tombstone == false){
                    self -> nodes[currIndex].tombstone = true;
                    self -> nodes[currIndex].use = 0;
                    self -> size -= 1;
                    map_node_t returnVal = self -> nodes[currIndex];
                    // unlock
                    pthread_mutex_unlock(&self -> write_lock);
                    return returnVal;
                }
            }
            oldIndex += 1;
        }
    }
    map_node_t node;
    node.key = MAP_KEY(NULL, 0);
    node.val = MAP_VAL(NULL, 0);
    node.tombstone = true;
    node.use = 0;

    // if here, then not found
    pthread_mutex_unlock(&self -> write_lock);
    return node;
}

bool clear_map(hashmap_t *self) {
	// check the param
    if (self == NULL){
        errno = EINVAL;
        return false;
    }

    // grab the mutex
    pthread_mutex_lock(&self -> write_lock);

    // check if invalid
    if (self -> invalid == true){
        errno = EINVAL;
        pthread_mutex_unlock(&self -> write_lock);
        return false;
    }

    // if not invalid, go through each node and destroy it if its tombstone status is false
    for (int i = 0; i < self -> capacity; i++){
        if (self -> nodes[i].tombstone == false && self -> nodes[i].key.key_len != 0){
            self -> destroy_function(self -> nodes[i].key, self -> nodes[i].val);
            self -> nodes[i].tombstone = true;
            self -> nodes[i].use = 0;
        }
    }

    // set size to 0
    self -> size = 0;

    pthread_mutex_unlock(&self -> write_lock);
    return true;
}

bool invalidate_map(hashmap_t *self) {
    return false;
}
