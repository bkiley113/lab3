#include "hash-table-base.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>

#include <pthread.h>

struct list_entry {
	const char *key;
	uint32_t value;
	SLIST_ENTRY(list_entry) pointers;
};

SLIST_HEAD(list_head, list_entry);

struct hash_table_entry {
	struct list_head list_head;
	//in order to potentially improve performance, fine grained locking!
	pthread_mutex_t mutex;
};

struct hash_table_v2 {
	struct hash_table_entry entries[HASH_TABLE_CAPACITY];
};

struct hash_table_v2 *hash_table_v2_create()
{
	struct hash_table_v2 *hash_table = calloc(1, sizeof(struct hash_table_v2));
	assert(hash_table != NULL);
	for (size_t i = 0; i < HASH_TABLE_CAPACITY; ++i) {
		struct hash_table_entry *entry = &hash_table->entries[i];
		SLIST_INIT(&entry->list_head);
		//each bucket of the hash table will have a lock initialized
		//instead of the whole hash table

		if(pthread_mutex_init(&entry->mutex, NULL) != 0){
			perror("pthread_mutex_init");
			exit(EXIT_FAILURE);
		}
	}
	return hash_table;
}

static struct hash_table_entry *get_hash_table_entry(struct hash_table_v2 *hash_table,
                                                     const char *key)
{
	assert(key != NULL);
	uint32_t index = bernstein_hash(key) % HASH_TABLE_CAPACITY;
	struct hash_table_entry *entry = &hash_table->entries[index];
	return entry;
}

static struct list_entry *get_list_entry(struct hash_table_v2 *hash_table,
                                         const char *key,
                                         struct list_head *list_head)
{
	assert(key != NULL);

	struct list_entry *entry = NULL;
	
	SLIST_FOREACH(entry, list_head, pointers) {
	  if (strcmp(entry->key, key) == 0) {
	    return entry;
	  }
	}
	return NULL;
}

bool hash_table_v2_contains(struct hash_table_v2 *hash_table,
                            const char *key)
{
	struct hash_table_entry *hash_table_entry = get_hash_table_entry(hash_table, key);
	struct list_head *list_head = &hash_table_entry->list_head;
	struct list_entry *list_entry = get_list_entry(hash_table, key, list_head);
	return list_entry != NULL;
}

void hash_table_v2_add_entry(struct hash_table_v2 *hash_table,
                             const char *key,
                             uint32_t value)
{
	struct hash_table_entry *hash_table_entry = get_hash_table_entry(hash_table, key);

	if(pthread_mutex_lock(&hash_table_entry->mutex) != 0){
		perror("pthread_mutex_lock");
		exit(EXIT_FAILURE);
	}

	struct list_head *list_head = &hash_table_entry->list_head;
	struct list_entry *list_entry = get_list_entry(hash_table, key, list_head);

	/* Update the value if it already exists */
	if (list_entry != NULL) {
		list_entry->value = value;
		if(pthread_mutex_unlock(&hash_table_entry->mutex) != 0){
			perror("pthread_mutex_unlock");
			exit(EXIT_FAILURE);
		}
		return;
	}

	list_entry = calloc(1, sizeof(struct list_entry));
	list_entry->key = key;
	list_entry->value = value;
	SLIST_INSERT_HEAD(list_head, list_entry, pointers);

	if(pthread_mutex_unlock(&hash_table_entry->mutex) != 0){
		perror("pthread_mutex_unlock");
		exit(EXIT_FAILURE);
	}
}

uint32_t hash_table_v2_get_value(struct hash_table_v2 *hash_table,
                                 const char *key)
{
	struct hash_table_entry *hash_table_entry = get_hash_table_entry(hash_table, key);
	struct list_head *list_head = &hash_table_entry->list_head;
	struct list_entry *list_entry = get_list_entry(hash_table, key, list_head);
	assert(list_entry != NULL);
	return list_entry->value;
}

void hash_table_v2_destroy(struct hash_table_v2 *hash_table)
{
	for (size_t i = 0; i < HASH_TABLE_CAPACITY; ++i) {
		struct hash_table_entry *entry = &hash_table->entries[i];
		struct list_head *list_head = &entry->list_head;
		struct list_entry *list_entry = NULL;
		while (!SLIST_EMPTY(list_head)) {
			list_entry = SLIST_FIRST(list_head);
			SLIST_REMOVE_HEAD(list_head, pointers);
			free(list_entry);
		}

		//free up the list entry's mutex
		if(pthread_mutex_destroy(&entry->mutex) != 0){
			perror("pthread_mutex_destroy");
			exit(EXIT_FAILURE);
		}
	}
	free(hash_table);
}


// #include "hash-table-base.h"

// #include <assert.h>
// #include <stdlib.h>
// #include <string.h>
// #include <sys/queue.h>
// #include <pthread.h>

// #define NUM_STRIPES 64

// struct list_entry {
//     const char *key;
//     uint32_t value;
//     SLIST_ENTRY(list_entry) pointers;
// };

// SLIST_HEAD(list_head, list_entry);

// struct hash_table_entry {
//     struct list_head list_head;
//     // Removed per-bucket mutex in favor of lock striping
// };

// struct hash_table_v2 {
//     struct hash_table_entry entries[HASH_TABLE_CAPACITY];
//     pthread_mutex_t stripes[NUM_STRIPES]; // Array of stripe locks
// };

// struct hash_table_v2 *hash_table_v2_create()
// {
//     struct hash_table_v2 *hash_table = calloc(1, sizeof(struct hash_table_v2));
//     assert(hash_table != NULL);
//     // Initialize each bucket's list
//     for (size_t i = 0; i < HASH_TABLE_CAPACITY; ++i) {
//         struct hash_table_entry *entry = &hash_table->entries[i];
//         SLIST_INIT(&entry->list_head);
//     }
//     // Initialize the stripe locks
//     for (int i = 0; i < NUM_STRIPES; i++) {
//         if (pthread_mutex_init(&hash_table->stripes[i], NULL) != 0) {
//             perror("pthread_mutex_init");
//             exit(EXIT_FAILURE);
//         }
//     }
//     return hash_table;
// }

// static struct hash_table_entry *get_hash_table_entry(struct hash_table_v2 *hash_table,
//                                                        const char *key)
// {
//     assert(key != NULL);
//     uint32_t index = bernstein_hash(key) % HASH_TABLE_CAPACITY;
//     return &hash_table->entries[index];
// }

// static struct list_entry *get_list_entry(struct hash_table_v2 *hash_table,
//                                          const char *key,
//                                          struct list_head *list_head)
// {
//     assert(key != NULL);
//     struct list_entry *entry = NULL;
//     SLIST_FOREACH(entry, list_head, pointers) {
//         if (strcmp(entry->key, key) == 0) {
//             return entry;
//         }
//     }
//     return NULL;
// }

// bool hash_table_v2_contains(struct hash_table_v2 *hash_table,
//                             const char *key)
// {
//     struct hash_table_entry *hash_table_entry = get_hash_table_entry(hash_table, key);
//     struct list_head *list_head = &hash_table_entry->list_head;
//     struct list_entry *list_entry = get_list_entry(hash_table, key, list_head);
//     return list_entry != NULL;
// }

// void hash_table_v2_add_entry(struct hash_table_v2 *hash_table,
//                              const char *key,
//                              uint32_t value)
// {
//     // Compute bucket index and corresponding stripe index
//     uint32_t bucket_index = bernstein_hash(key) % HASH_TABLE_CAPACITY;
//     uint32_t stripe_index = bucket_index % NUM_STRIPES;

//     // Lock the stripe corresponding to the bucket
//     if (pthread_mutex_lock(&hash_table->stripes[stripe_index]) != 0) {
//         perror("pthread_mutex_lock");
//         exit(EXIT_FAILURE);
//     }

//     // Get the target bucket and its list
//     struct hash_table_entry *hash_table_entry = &hash_table->entries[bucket_index];
//     struct list_head *list_head = &hash_table_entry->list_head;
//     struct list_entry *list_entry = get_list_entry(hash_table, key, list_head);

//     /* Update the value if it already exists */
//     if (list_entry != NULL) {
//         list_entry->value = value;
//         if (pthread_mutex_unlock(&hash_table->stripes[stripe_index]) != 0) {
//             perror("pthread_mutex_unlock");
//             exit(EXIT_FAILURE);
//         }
//         return;
//     }

//     // Create a new list entry and insert it at the head of the list
//     list_entry = calloc(1, sizeof(struct list_entry));
//     list_entry->key = key;
//     list_entry->value = value;
//     SLIST_INSERT_HEAD(list_head, list_entry, pointers);

//     if (pthread_mutex_unlock(&hash_table->stripes[stripe_index]) != 0) {
//         perror("pthread_mutex_unlock");
//         exit(EXIT_FAILURE);
//     }
// }

// uint32_t hash_table_v2_get_value(struct hash_table_v2 *hash_table,
//                                  const char *key)
// {
//     struct hash_table_entry *hash_table_entry = get_hash_table_entry(hash_table, key);
//     struct list_head *list_head = &hash_table_entry->list_head;
//     struct list_entry *list_entry = get_list_entry(hash_table, key, list_head);
//     assert(list_entry != NULL);
//     return list_entry->value;
// }

// void hash_table_v2_destroy(struct hash_table_v2 *hash_table)
// {
//     for (size_t i = 0; i < HASH_TABLE_CAPACITY; ++i) {
//         struct hash_table_entry *entry = &hash_table->entries[i];
//         struct list_head *list_head = &entry->list_head;
//         struct list_entry *list_entry = NULL;
//         while (!SLIST_EMPTY(list_head)) {
//             list_entry = SLIST_FIRST(list_head);
//             SLIST_REMOVE_HEAD(list_head, pointers);
//             free(list_entry);
//         }
//     }
//     // Destroy each stripe lock
//     for (int i = 0; i < NUM_STRIPES; i++) {
//         if (pthread_mutex_destroy(&hash_table->stripes[i]) != 0) {
//             perror("pthread_mutex_destroy");
//             exit(EXIT_FAILURE);
//         }
//     }
//     free(hash_table);
// }
