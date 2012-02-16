#include "Rand.h"

#define LL  37                     /* the short lag */
#define MM (1L<<30)                 /* the modulus */
#define mod_diff(x,y) (((x)-(y))&(MM-1)) /* subtraction mod MM */

long arr_dummy = -1;

void
RandomGenerator::ran_array(long aa[], int n)
{
	register int i,j;
	for (j = 0; j < KK; j++)
		aa[j] = ran_x[j];
	for (; j < n; j++)
		aa[j] = mod_diff(aa[j-KK], aa[j-LL]);
	for (i = 0; i < LL; i++, j++)
		ran_x[i] = mod_diff(aa[j-KK], aa[j-LL]);
	for (; i < KK; i++, j++)
		ran_x[i] = mod_diff(aa[j-KK], ran_x[i-LL]);
}


#define TT  70   /* guaranteed separation between streams */
#define is_odd(x)  ((x)&1)          /* units bit of x */

RandomGenerator::RandomGenerator(long seed)
{
	register int t,j;
	long x[KK + KK - 1];              /* the preparation buffer */
	register long ss = (seed + 2) & (MM - 2);
	for (j = 0; j < KK; j++) {
		x[j] = ss;                      /* bootstrap the buffer */
		ss <<= 1;
		if (ss>=MM) ss -= MM-2; /* cyclic shift 29 bits */
	}
	x[1]++;              /* make x[1] (and only x[1]) odd */
	for (ss = seed & (MM-1), t = TT - 1; t; ) {       
		for (j = KK - 1; j > 0; j--)
			x[j + j] = x[j],
			x[j + j - 1] = 0; /* "square" */
		for (j = KK + KK - 2; j >= KK; j--)
			x[j - (KK - LL)] = mod_diff(x[j - (KK - LL)], x[j]),
			x[j - KK] = mod_diff(x[j - KK], x[j]);
		if (is_odd(ss)) {              /* "multiply by z" */
			for (j = KK; j > 0; j--)
				x[j] = x[j-1];
			x[0] = x[KK];            /* shift the buffer cyclically */
			x[LL] = mod_diff(x[LL], x[KK]);
		}
		if (ss)
			ss >>= 1;
		else
			t--;
	}
	for (j = 0; j < LL; j++)
		ran_x[j + KK - LL] = x[j];
	for (; j < KK; j++)
		ran_x[j - LL] = x[j];
	for (j = 0; j < 10; j++)
		ran_array(x, KK + KK - 1); /* warm things up */
	arr_ptr = &arr_dummy;
}


long
RandomGenerator::arr_cycle()
{
	ran_array(arr_buf, QUALITY);
	arr_buf[KK] = -1;
	arr_ptr = arr_buf + 1;
	return arr_buf[0];
}


double
RandomGenerator::nextDouble() 
{
	return (((long long)nextBits(26) << 27) + nextBits(27))
         / (double)((long long)1 << 53);
}


float
RandomGenerator::nextFloat()
{
	return nextBits(24) / ((float)(1 << 24));
}