#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>

int main() {

    int i;
    int off;
    
    FILE *f;
    
    f = fopen("bitfield.struct.inc", "w");
    for (off = 0; off < 8; off++) {
        for (i=1; i<=64; i++) {
            int nbits;
            
            /*
            Packed bit-fields of type char were not properly bit-packed on many targets prior to GCC 4.4
            */
            // if (i <= 8) nbits = 8;
            if (i <= 16) nbits = 16;
            else if (i <= 32) nbits = 32;
            else nbits = 64;
            fprintf(f, "struct off%di%d { long : %d; int%d_t  data : %d; } __attribute__ ((packed));\n", off, i, off, nbits, i);
            fprintf(f, "struct off%du%d { long : %d; uint%d_t data : %d; } __attribute__ ((packed));\n", off, i, off, nbits, i);
        }
    }
    fclose(f);
    
    int is_signed;
    
    f = fopen("bitfield.function.get.inc", "w");
    for (i=1; i<=64; i++) {
        for (off = 0; off < 8; off++) {
            for (is_signed = 0; is_signed < 2; is_signed++) {
                fprintf(f, "case %d: return ((struct off%d%c%d *) data)->data;\n", i * 16 + off * 2 + is_signed, off, is_signed ? 'i' : 'u', i);
            }
        }
    }
    
    f = fopen("bitfield.function.set.inc", "w");
    for (i=1; i<=64; i++) {
        for (off = 0; off < 8; off++) {
            for (is_signed = 0; is_signed < 2; is_signed++) {
                fprintf(f, "case %d: { int64_t ov = ((struct off%d%c%d *) data)->data; ((struct off%d%c%d *) data)->data = value; return ov; }\n", i * 16 + off * 2 + is_signed, off, is_signed ? 'i' : 'u', i, off, is_signed ? 'i' : 'u', i);
            }
        }
    }

    
    fclose(f);
    
}
