#include <stdint.h>


#include "bitfield.struct.inc"


int64_t bitfield_get(void *data, int is_signed, int offset, int nbits) {
    
    
/*
    +-+-+-+-+-+-+-+-++-+-+-+-+-+-+-+-++-+-+-+-+-+-+-+-|
    |a|b|c|d|e|f|g|h||i|j|k|l|m|n|o|p||q|r|s|t|u|v|w|x|
    +-+-+-+-+-+-+-+-++-+-+-+-+-+-+-+-++-+-+-+-+-+-+-+-|
    ^
     \--- data
    
    offset=3, nbits=10 取得的是 defghijklm
    
    如果is_signed，则m为符号位。
    
    单元测试（第一组）：
    int x = 1, y = 2;
    bitfield_get(&x, 1, 0, 3) 应该返回1
    bitfield_get(&y, 1, 0, 2) 应该返回-2
    bitfield_get(&y, 0, 0, 2) 应该返回2
    bitfield_get(&y, 1, 1, 3) 应该返回1
    bitfield_get(&y, 1, 1, 3) 应该返回1
    bitfield_get(&x, 1, 0, 1) 应该返回-1
    bitfield_get(&x, 0, 0, 1) 应该返回1
    
    还需要测试跨字节的情况
*/
    
    data += offset / 8;
    offset %= 8;
    
    switch (nbits * 16 + offset * 2 + is_signed) {
#include "bitfield.function.get.inc"    
    }
    return 0;
}

int64_t bitfield_set(void *data, int is_signed, int offset, int nbits, int64_t value) {
    // printf("bitfield_set %p %d %d %d %d\n", data, is_signed, offset, nbits, value);
    data += offset / 8;
    offset %= 8;
    
    switch (nbits * 16 + offset * 2 + is_signed) {
#include "bitfield.function.set.inc"    
    }
    return 0;
}
