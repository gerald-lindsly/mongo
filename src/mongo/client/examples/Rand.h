#ifndef __RAND_H__
#define __RAND_H__

/*    This program by D E Knuth is in the public domain and freely copyable.
 *    It is explained in Seminumerical Algorithms, 3rd edition, Section 3.6
 *    (or in the errata to the 2nd edition --- see
 *        http://www-cs-faculty.stanford.edu/~knuth/taocp.html
 *    in the changes to Volume 2 on pages 171 and following).              */

/*    N.B. The MODIFICATIONS introduced in the 9th printing (2002) are
      included here; there's no backwards compatibility with the original. */

/*    If you find any bugs, please report them immediately to
 *                 taocp@cs.stanford.edu
 *    (and you will be rewarded if the bug is genuine). Thanks!            */

/************ see the book for explanations and caveats! *******************/
/************ in particular, you need two's complement arithmetic **********/

// adapted to C++ by Gerald Lindsly (2012)

class RandomGenerator
{
public:
    RandomGenerator(long seed=314159L);

    inline long next() { return *arr_ptr >= 0 ? *arr_ptr++ : arr_cycle(); }

    inline long nextBits(unsigned n) { return (0x3FFFFFFF >> (30 - n)) & next(); }

    double nextDouble();
    float  nextFloat();

protected:
    long *arr_ptr; /* the next random number, or -1 */
    long arr_cycle();

    static const int KK=100;  /* the long lag */
    long ran_x[KK];    /* the generator state */

    void ran_array(long aa[], int n);

    static const int QUALITY=1009; /* recommended quality level for high-res use */
    long arr_buf[QUALITY];
};


#endif
