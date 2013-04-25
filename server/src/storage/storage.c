/* © Copyright 2011 jingmi. All Rights Reserved.
 *
 * +----------------------------------------------------------------------+
 * | data block store                                                     |
 * +----------------------------------------------------------------------+
 * | Author: mi.jing@jiepang.com                                          |
 * +----------------------------------------------------------------------+
 * | Created: 2011-11-04 12:51                                            |
 * +----------------------------------------------------------------------+
 */

#include "storage.h"

/* TODO
 * 1. interface - DONE
 * 2. lock - HALF DONE(not thread-safe)
 * 3. sync header - DONE
 * 4. split functions & log
 * 5. bdb transaction
 */

/* NOTE key is supposed to be 8 bytes while logging */

/** 
 * @brief Max amount of block types
 */
#define MAX_BLOCK_CNT (20)

STATIC_MODIFIER bool inited_global_vars = false;

/** 
 * @brief Size of each block
 */
STATIC_MODIFIER size_t block_size_array[MAX_BLOCK_CNT] = {0};

/** 
 * @brief Array of block files' info
 */
STATIC_MODIFIER struct block_file_t block_filehandler_array[MAX_BLOCK_CNT];

/** 
 * @brief Count of block types, computed in init_storage()
 */
STATIC_MODIFIER size_t block_type_count;

/** 
 * @brief Smallest block size
 */
#define BEGINNING_SIZE (128)

/** 
 * @brief Largest block size: 4M
 */
#define LARGEST_SIZE (1024 * 1024 * 4)

#define EX_LOCK do { rwlock_wrlock(&(fh->lock)); } while (0)
#define EX_UNLOCK do { rwlock_unwrlock(&(fh->lock)); } while (0)
#define SH_LOCK do { rwlock_rdlock(&(fh->lock)); } while (0)
#define SH_UNLOCK do { rwlock_unrdlock(&(fh->lock)); } while (0)

#define EX_LOCK_IDX(_bsp) do { rwlock_wrlock(_bsp->index_lock); } while (0)
#define EX_UNLOCK_IDX(_bsp) do { rwlock_unwrlock(_bsp->index_lock); } while (0)
#define SH_LOCK_IDX(_bsp) do { rwlock_rdlock(_bsp->index_lock); } while (0)
#define SH_UNLOCK_IDX(_bsp) do { rwlock_unrdlock(_bsp->index_lock); } while (0)

STATIC_MODIFIER int
read_data_file(size_t block_id, void *buffer, size_t data_size, off_t offset)
{
    int ret;
    struct block_file_t *fh;

    assert(buffer != NULL);
    assert(block_id < block_type_count);

    fh = &block_filehandler_array[block_id];
    ret = 0;

    if (pread(fh->fd, buffer, data_size, offset) != (ssize_t)data_size)
    {
        ret = -1;
        log_warn("cannot read data from file: %zu.dat: %s", block_size_array[block_id], strerror(errno));
        goto err;
    }

err:
    return ret;
}

/* TODO add an argu to specify whether flush data immediately */
STATIC_MODIFIER int
sync_data_file(size_t block_id, void *data, size_t data_size, off_t offset)
{
    int ret;
    struct block_file_t *fh;

    assert(data != NULL);
    assert(block_id < block_type_count);

    fh = &block_filehandler_array[block_id];
    ret = 0;

    if (pwrite(fh->fd, data, data_size, offset) != (ssize_t)data_size)
    {
        ret = -1;
        log_warn("cannot write data to file: %zu.dat: %s", block_size_array[block_id], strerror(errno));
        goto err;
    }
    // fdatasync(fh->fd);

err:
    return ret;
}

STATIC_MODIFIER int
sync_data_file_header(size_t block_id, bool sync_flag __attribute__((unused)))
{
    struct block_file_t *fh;

    fh = &block_filehandler_array[block_id];
    sync_data_file(block_id,
                   &fh->block_size,
                   __offset_of(block_file_t, free_slots_offset) - __offset_of(block_file_t, block_size),
                   0);

    return 0;
}

STATIC_MODIFIER int
load_data_file(const char *filename, size_t block_id)
{
    int fd;
    int ret;
    struct block_file_t *fh;
    size_t header_size;

    ret = 0;
    header_size = sizeof(struct block_file_t) - __offset_of(struct block_file_t, block_size);
    fd = open(filename, O_RDWR | O_CLOEXEC);
    if (fd < 0)
    {
        ret = -1;
        log_error("cannot open data file - %s: %s", filename, strerror(errno));
        goto err;
    }
    fh = &block_filehandler_array[block_id];
    fh->fd = fd;

    ret = read_data_file(block_id, (void*)&(fh->block_size), header_size, 0);
    if (ret != 0)
    {
        goto err;
    }
    log_notice("load data file successfully: %s", filename);

err:
    return ret;
}

STATIC_MODIFIER int
create_data_file(const char *filename, size_t block_id)
{
    int fd;
    int ret;
    struct block_file_t *fh;
    void *buffer;
    size_t bufsize;
    ssize_t nwrite;

    ret = 0;
    fd = open(filename, O_CREAT | O_RDWR | O_CLOEXEC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd < 0)
    {
        ret = -1;
        log_error("cannot open data file - %s: %s", filename, strerror(errno));
        goto err;
    }
    fh = &block_filehandler_array[block_id];
    fh->fd = fd;
    rwlock_build(&(fh->lock));
    fh->block_size = block_size_array[block_id];
    fh->block_cnt = 0;
    fh->free_slots_cnt = 0;

    buffer = (void*)fh + __offset_of(block_file_t, block_size);
    bufsize = sizeof(struct block_file_t) - __offset_of(block_file_t, block_size);
    nwrite = write(fh->fd, buffer, bufsize);
    if (nwrite != (ssize_t)bufsize)
    {
        ret = -1;
        log_error("write data file header failed - %s: %s", filename, strerror(errno));
        goto err;
    }
    // fdatasync(fh->fd);
    log_notice("create data file successfully: %s", filename);

err:
    return ret;
}

STATIC_MODIFIER int
find_block_file(size_t block_size)
{
    for (size_t i=0; i<block_type_count; i++)
    {
        if (block_size == block_size_array[i])
            return i;
    }

    log_warn("invalid block_size: %zu", block_size);
    return -1;
}

STATIC_MODIFIER int
append_block(struct bucket_storage_t *bsp, size_t block_id, void *key, size_t key_size, void *data, size_t data_size)
{
    int ret;
    struct block_file_t *fh;
    off_t offset;
    union index_key_t index_key;
    struct index_value_t index_value;
    uint32_t crc32_value;

    assert(bsp != NULL);
    ret = 0;
    if (block_id >= block_type_count)
    {
        log_warn("invalid block_id: %zu", block_id);
        ret = -1;
        goto err;
    }

    fh = &(block_filehandler_array[block_id]);
    assert(fh->fd > 0);
    assert(data_size <= block_size_array[block_id]);
    crc32_value = crc32_calc(data, data_size);
    index_key.key_num = 0;
    key_size = (key_size >= KEY_LEN ? KEY_LEN : key_size);
    memcpy(&index_key.key, key, key_size);

    EX_LOCK_IDX(bsp);
    EX_LOCK;
    /* MUST sync data file before index file */
    if (fh->free_slots_cnt != 0)
    {
        /* use free slots */
        offset = fh->free_slots_offset[ fh->free_slots_cnt-1 ];
        fh->free_slots_offset[ fh->free_slots_cnt-1 ] = 0;
        -- fh->free_slots_cnt;
        fh->block_cnt ++;
        sync_data_file_header(block_id, false);
        sync_data_file(block_id, data, data_size, offset);
        log_debug("write free slot %zu.dat, free_slot = %zu, key = 0x%lx, data_size = %zu, offset = %zu, data crc32 = %u",
                block_size_array[block_id], fh->free_slots_cnt,
                index_key.key_num, data_size, offset, crc32_value);
    }
    else
    {
        fh->block_cnt ++;
        sync_data_file_header(block_id, false);
        offset = DATA_SECTION_POS + fh->block_size * (fh->block_cnt - 1);
        sync_data_file(block_id, data, data_size, offset);
        log_debug("append %zu.dat, key = 0x%lx, data_size = %zu, offset = %zu, data crc32 = %u",
                block_size_array[block_id], index_key.key_num, data_size, offset, crc32_value);
    }
    EX_UNLOCK;

    index_value.crc32     = crc32_value;
    index_value.block_id  = (uint32_t)block_id;
    index_value.offset    = offset;
    index_value.data_size = data_size;
    
    if ((offset - 800024) & 0b1111111) {
        log_error("offset %zu is not aligned to 128 bytes", offset);
        exit(1);
    }
    
    if (insert_record(bsp->indexdb, &index_key, sizeof index_key, &index_value, sizeof index_value) == 0)
    {
        log_debug("append index successfully, key = 0x%lx, data crc32 = %u", index_key.key_num, crc32_value);
    }
    else
    {
        /* no need to log here since we log mesg in insert_record() */
        goto err;
    }

err:
    EX_UNLOCK_IDX(bsp);

    return ret;
}

STATIC_MODIFIER int
modify_block(struct bucket_storage_t *bsp, struct index_value_t *iv, void *key, size_t key_size, void *data, size_t data_size)
{
    int ret;
    struct block_file_t *fh;
    off_t offset;
    union index_key_t index_key;
    uint32_t crc32_value;
    int retno;
    size_t block_id;

    assert(bsp != NULL);
    block_id = iv->block_id;
    ret = 0;
    if (block_id >= block_type_count)
    {
        log_warn("invalid block_id: %zu", block_id);
        ret = -1;
        goto err;
    }

    fh = &(block_filehandler_array[block_id]);
    assert(fh->fd > 0);
    assert(data_size <= block_size_array[block_id]);
    crc32_value = crc32_calc(data, data_size);
    index_key.key_num = 0;
    key_size = (key_size >= KEY_LEN ? KEY_LEN : key_size);
    memcpy(&index_key.key, key, key_size);

    offset = iv->offset;

    EX_LOCK;
    retno = sync_data_file(block_id, data, data_size, offset);
    assert(retno == 0);
    log_debug("modify %zu.dat, key = 0x%lx, data_size = %zu, offset = %zu, data crc32 = %u",
            block_size_array[block_id], index_key.key_num, data_size, offset, crc32_value);
    EX_UNLOCK;

    iv->crc32 = crc32_value;
    iv->data_size = data_size;

    EX_LOCK_IDX(bsp);
    retno = insert_record(bsp->indexdb, key, key_size, iv, sizeof(*iv));
    if (retno != 0)
    {
        ret = -1;
        log_warn("modify index failed");
        goto err;
    }

err:
    EX_UNLOCK_IDX(bsp);
    return ret;
}

STATIC_MODIFIER int
calc_block_size(size_t data_size)
{
    int size;

    size = BEGINNING_SIZE;
    for (int i=0; i<MAX_BLOCK_CNT; i++)
    {
        if (size >= (int)data_size)
            return size;
        size *= 2;
    }

    return -1;
}

/* time window in this function, so it's not thread-safe actually*/
int
update_block(struct bucket_storage_t *bsp, void *key, size_t key_size, void *data, size_t data_size)
{
   int ret;
   int retno;
   DBT dbt;
   DBT *dbt_ptr;
   size_t value_len;
   struct index_value_t *iv;
   ssize_t new_block_id; /* use ssize_t to express negative */
   char error_log[1024] = {0};
   pre_timer();

   assert(bsp != NULL);
   launch_timer();
   ret = 0;
   value_len = sizeof(struct index_value_t);
   dbt_ptr = &dbt;

   /* FIXME race condition here */
   retno = find_record(bsp->indexdb, &dbt_ptr, key, key_size, &value_len);
   if (retno == 0)
   {
       iv = dbt.data;
       if (data_size <= block_size_array[ iv->block_id ])
       {
           retno = modify_block(bsp, iv, key, key_size, data, data_size);
           if (retno != 0)
           {
               ret = -1;
               strcpy(error_log, "update block failed");
               goto err;
           }
       }
       else
       {
           /* TODO put append_block() executed before delete(but not delete_block()) */
           retno = delete_block(bsp, key, key_size);
           if (retno != 0)
           {
               ret = -2;
               strcpy(error_log, "delete failed while updating block");
               goto err;
           }
           new_block_id = find_block_file(calc_block_size(data_size));
           retno = append_block(bsp, new_block_id, key, key_size, data, data_size);
           if (retno != 0)
           {
               ret = -3;
               log_error("Append block failed while updating block. Might lost record!!!");
               goto err;
           }
       }
   }
   else if (retno == DB_NOTFOUND)
   {
       /* TODO add test suites for this case */
       new_block_id = find_block_file(calc_block_size(data_size));
       if (new_block_id < 0)
       {
           snprintf(error_log, sizeof error_log, "invalid data size: %zu", data_size);
           goto err;
       }
       retno = append_block(bsp, new_block_id, key, key_size, data, data_size);
       if (retno != 0)
       {
           ret = -4;
           strcpy(error_log, "Append block failed while add new block");
           goto err;
       }
   }
   else
   {
       snprintf(error_log, sizeof error_log, "update failed: %d", retno);
       ret = -1;
       goto err;
   }

err:
   if (dbt.data != NULL)
       slab_free(dbt.data);

   stop_timer();
   if (ret == 0)
   {
       log_debug("update block successfully, took %ld.%06ld sec", used.tv_sec, used.tv_usec);
   }
   else
   {
       log_warn("%s, took %ld.%06ld sec", error_log, used.tv_sec, used.tv_usec);
   }

   return ret;
}

/* TODO split find_block() to 2 functions: find_block() & do_find_block()
 * log info in find_block()
 */
int
find_block(struct bucket_storage_t *bsp __attribute__((unused)), void *key, size_t key_size, void **value_ptr, size_t *value_len)
{
    DBT dbt;
    DBT *dbt_ptr;
    int ret, retno;
    size_t vlen;
    pre_timer();

    assert(bsp != NULL);
    /* TODO make value_ptr to use caller's memory if value_ptr is not NULL */
    launch_timer();
    dbt_ptr = &dbt;
    *value_ptr = NULL;
    *value_len = 0;
    vlen = sizeof(struct index_value_t);
    ret = 0;

    /* TODO seems that bdb transaction is thread-safe without DB_THREAD */
    /* FIXME time window here, might get corrupt block address between find_record() and pread() */
    SH_LOCK_IDX(bsp);
    retno = find_record(bsp->indexdb, &dbt_ptr, key, key_size, &vlen);
    if (retno == 0)
    {
        ssize_t nread;
        struct index_value_t *iv;
        struct block_file_t *fh;

        iv = dbt.data;
        fh = &block_filehandler_array[ iv->block_id ];
        *value_len = iv->data_size;
        if ((*value_ptr = slab_alloc(iv->data_size)) == NULL)
        {
            log_error("no memory");
            ret = -1;
            goto err;
        }
        
        SH_LOCK;
        nread = pread(fh->fd, *value_ptr, iv->data_size, iv->offset);
        SH_UNLOCK;
        if (nread != (ssize_t)iv->data_size)
        {
            log_warn("cannot read data: %d - %s", errno, strerror(errno));
            goto err;
        }
    }
    else
    {
        /* retno == DB_NOTFOUND or NOT */
        /* TODO log if error occur*/
        ret = retno;
        goto err;
    }

err:
    SH_UNLOCK_IDX(bsp);
    stop_timer();
    if (ret == 0)
    {
        log_debug("search successfully, key = 0x%lX, took %ld.%06ld sec", *(uint64_t*)key, used.tv_sec, used.tv_usec);
    }
    else
    {
        log_debug("search failed, key = 0x%lX, took %ld.%06ld sec", *(uint64_t*)key, used.tv_sec, used.tv_usec);
    }

    return ret;
}

int
delete_block(struct bucket_storage_t *bsp __attribute__((unused)), void *key, size_t key_size)
{
    struct index_value_t *iv = NULL;
    struct block_file_t *fh = NULL;
    DBT dbt;
    DBT *dbt_ptr;
    int ret, retno;
    size_t vlen;
    size_t slot_pos;
    pre_timer();

    assert(bsp != NULL);
    launch_timer();
    dbt_ptr = &dbt;
    vlen = sizeof(struct index_value_t);
    ret = 0;

    /* TODO make this function to be thread-safe */
    retno = find_record(bsp->indexdb, &dbt_ptr, key, key_size, &vlen);
    if (retno == 0)
    {
        iv = dbt.data;
        fh = &block_filehandler_array[ iv->block_id ];
        if ((retno = del_record(bsp->indexdb, key, key_size)) != 0)
        {
            log_warn("delete index failed: %d - %s", retno, db_strerror(retno));
            ret = -1;
            goto err;
        }
        /* XXX Safe? */
        EX_LOCK;
        if (fh->free_slots_cnt >= MAX_FREE_SLOTS)
        {
            EX_UNLOCK;
            log_error("free slots if not enough in %zu.dat, data_size = %zu, offset = %zu",
                    block_size_array[iv->block_id], iv->data_size, iv->offset);
            goto err;
        }
        -- fh->block_cnt;
        slot_pos = fh->free_slots_cnt;
        fh->free_slots_offset[ slot_pos ] = iv->offset;
        ++ fh->free_slots_cnt;
        EX_UNLOCK;
        sync_data_file(iv->block_id, &(fh->block_size),
                __offset_of(block_file_t, free_slots_offset) - __offset_of(block_file_t, block_size),
                0);
        sync_data_file(iv->block_id, &(fh->free_slots_offset[ slot_pos ]), sizeof(fh->free_slots_offset[0]),
                    SLOTS_SECTION_POS + sizeof(fh->free_slots_offset[0]) * slot_pos);
    }
    else if (retno == DB_NOTFOUND)
    {
        ret = DB_NOTFOUND;
        goto err;
    }
    else
    {
        ret = -1;
        goto err;
    }

err:
    if (dbt.data != NULL)
        slab_free(dbt.data);

    stop_timer();
    if (ret == 0)
    {
        log_debug("delete record successfully: file = %zu.dat, free_slot = %zu, data_size = %zu, offset = %zu, data crc32 = %u, took %ld.%06ld sec",
                fh->block_size, fh->free_slots_cnt, iv->data_size, iv->offset, iv->crc32, used.tv_sec, used.tv_usec);
    }
    else if (ret != DB_NOTFOUND)
    {
        log_warn("delete index failed: %d - %s, took %ld.%06ld sec", retno, db_strerror(retno), used.tv_sec, used.tv_usec);
    }

    return ret;
}

STATIC_MODIFIER int
open_index_file(const char *dir_name, const char *file_name, struct bucket_storage_t *bsp)
{
    int ret;
    struct bdb_t *bdbp;

    assert(bsp != NULL);

    ret = 0;
    bdbp = open_transaction_bdb(dir_name, file_name, 0, 0, NULL);
    if (bdbp == NULL)
    {
        log_error("cannot open index file");
        ret = -1;
        goto err;
    }

    if (bdbp->error_no != 0)
    {
        log_error("cannot open index file: %s", db_strerror(bdbp->error_no));
        ret = -1;
        goto err;
    }
    log_debug("index file open successfully");

    bsp->indexdb = bdbp;
    bsp->index_lock = rwlock_new();
err:
    return ret;
}

STATIC_MODIFIER int
open_data_files(const char *dir_name)
{
    int ret;
    int retno;
    char filename[PATH_MAX];

    ret = 0;
    for (size_t i=0; i<block_type_count; i++)
    {
        snprintf(filename, sizeof filename, "%s/%zu.dat", dir_name, block_size_array[i]);
        if ((retno=access(filename, R_OK | W_OK)) == 0)
        {
            if (load_data_file(filename, i) != 0)
            {
                ret = -1;
                goto err;
            }
        }
        else
        {
            if (create_data_file(filename, i) != 0)
            {
                ret = -1;
                goto err;
            }
        }
    }

err:
    return ret;
}

STATIC_MODIFIER int
init_storage(const char *dir_path __attribute__((unused)), size_t largest_size)
{
    int ret;
    size_t curr_size;

    ret = 0;
    if (largest_size < BEGINNING_SIZE)
        largest_size = LARGEST_SIZE;
    block_type_count = 0;
    memset(block_filehandler_array, 0, sizeof(block_filehandler_array));
    memset(block_size_array, 0, sizeof(block_size_array));

    curr_size = BEGINNING_SIZE;
    for (int i = 0; curr_size <= largest_size; i++)
    {
        if (i >= MAX_BLOCK_CNT)
        {
            log_error("init storage failed: largest block size exceeded the max limitation");
            ret = -1;
            break;
        }
        block_size_array[i] = curr_size;
        curr_size <<= 1;

        block_type_count = i + 1;
    }
    log_debug("init storage successfully");

    return ret;
}

int
open_storage(const char *dir_path, size_t largest_size, struct bucket_storage_t **bsp)
{
    int retno;
    int ret;
    struct stat sb;
    char *dir_name_copy = NULL, *file_name_copy = NULL;
    char *dir_name, *file_name;

    ret = 0;
    assert(bsp != NULL);
    *bsp = NULL;

    *bsp = slab_calloc(sizeof(struct bucket_storage_t));
    if (*bsp == NULL)
    {
        log_error("memory is not enough");
        ret = -4;
        goto err;
    }

    dir_name_copy  = strdup(dir_path);
    file_name_copy = strdup(dir_path);
    dir_name  = dirname(dir_name_copy);
    file_name = basename(file_name_copy);
    if ((stat(dir_name, &sb) != 0) && (errno == ENOENT))
    {
        retno = mkdir(dir_name, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXGRP);
        if (retno != 0)
        {
            log_error("cannot mkdir: %s, error: %d - %s", dir_name, errno, strerror(errno));
            ret = -5;
            goto err;
        }
    }
    else if (stat(dir_name, &sb) != 0)
    {
        log_error("cannot stat dir: %s, error: %d - %s", dir_name, errno, strerror(errno));
        ret = -5;
        goto err;
    }

    if (!inited_global_vars)
    {
        inited_global_vars = true;

        retno = init_storage(dir_name, largest_size);
        if (retno != 0)
        {
            ret = -1;
            goto err;
        }
        retno = open_data_files(dir_name);
        if (retno != 0)
        {
            ret = -2;
            goto err;
        }
    }

    retno = open_index_file(dir_name, file_name, *bsp);
    if (retno != 0)
    {
        ret = -3;
        goto err;
    }

err:
    if (ret == 0)
        log_debug("open storage successfully");
    else
    {
        if (*bsp != NULL)
            slab_free(*bsp);
        log_error("open storage failed: %d", ret);
    }

    free(dir_name_copy);
    free(file_name_copy);

    return ret;
}

/** 
 * @brief Close all file handlers to data file, sync header info to disk before
 * exists, then set the fd to -1
 * 
 * @return 0 if success, otherwise -1
 */
int
close_storage(struct bucket_storage_t *bsp)
{
    struct block_file_t *fh;
    int ret;
    int closed_file_cnt;
    int retno;

    assert(bsp != NULL);
    ret = 0;
    closed_file_cnt = 0;

    if (inited_global_vars)
    {
        for (size_t i=0; i<block_type_count; i++)
        {
            fh = &block_filehandler_array[i];
            if (fh->fd > 0)
            {
                void *buffer_ptr = (void*)fh + __offset_of(block_file_t, block_size);
                size_t bufsize = sizeof(struct block_file_t) - __offset_of(block_file_t, block_size);
                sync_data_file(i, buffer_ptr, bufsize, 0);
                close(fh->fd);
                fh->fd = -1;
                closed_file_cnt ++;
                log_debug("closed data file: %zu.dat", block_size_array[i]);
            }
            else
            {
                log_error("unreachable code section, block_id = %zu", i);
                ret = -1;
                goto err;
            }
        }
        block_type_count -= closed_file_cnt;
        inited_global_vars = false;
    }

    if (bsp->indexdb != NULL)
    {
        retno = close_bdb(bsp->indexdb);
        if (retno != 0)
        {
            ret = -1;
            log_error("close_bdb failed: (%d) %s", retno, db_strerror(retno));
            goto err;
        }
        bsp->indexdb = NULL;
    }

    slab_free(bsp);

err:
    return ret;
}

/* {{{ for testing only */

#ifdef TESTMODE

STATIC_MODIFIER void
testing_fetch_internal_data(size_t **block_size_array_ptr, size_t *block_type_count_ptr,
        struct block_file_t **block_filehandler_array_ptr)
{
    *block_size_array_ptr = block_size_array;
    *block_type_count_ptr = block_type_count;
    *block_filehandler_array_ptr = block_filehandler_array;
}

STATIC_MODIFIER void
increase_block_type_count(void)
{
    block_type_count ++;
}

STATIC_MODIFIER size_t *
get_block_type_count()
{
    return &block_type_count;
}

#endif

/* }}} */

/* vim: set expandtab tabstop=4 shiftwidth=4 foldmethod=marker: */
