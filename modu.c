#include <stdio.h>
int main()
{
   long long int a;
   int b;
   int bb;
   int c;
   long long int i;
   printf("Enter numer \n");
   scanf("%lld", &a);
   printf("Enter stride \n");
   scanf("%d", &bb);
   printf("Enter denom \n");
   scanf("%d", &b);
   i=a;
   while (i >= 0) {
     c=(i/bb)%b;
     printf("modulus %lld stride %d over %d = %d , numer / stride %lld\n", i,bb,b,c,i/bb);
     i--;
   }
}
