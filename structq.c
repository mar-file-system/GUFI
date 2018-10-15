/*
This file is part of GUFI, which is part of MarFS, which is released
under the BSD license.


Copyright (c) 2017, Los Alamos National Security (LANS), LLC
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----
NOTE:
-----

GUFI uses the C-Thread-Pool library.  The original version, written by
Johan Hanssen Seferidis, is found at
https://github.com/Pithikos/C-Thread-Pool/blob/master/LICENSE, and is
released under the MIT License.  LANS, LLC added functionality to the
original work.  The original work, plus LANS, LLC added functionality is
found at https://github.com/jti-lanl/C-Thread-Pool, also under the MIT
License.  The MIT License can be found at
https://opensource.org/licenses/MIT.


From Los Alamos National Security, LLC:
LA-CC-15-039

Copyright (c) 2017, Los Alamos National Security, LLC All rights reserved.
Copyright 2017. Los Alamos National Security, LLC. This software was produced
under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
the U.S. Department of Energy. The U.S. Government has rights to use,
reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR LOS
ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR
ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is
modified to produce derivative works, such modified software should be
clearly marked, so as not to confuse it with the version available from
LANL.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/



#include "structq.h"

#include <stdlib.h>
#include <stdio.h>
#include <strings.h>


struct Node
{
        struct work swork;
        struct Node* next;
}*rear, *front;

int qent = 0;

void delQueue()
{
      struct Node *temp, *var=rear;
      if(var==rear)
      {
             rear = rear->next;
             free(var);
             if (rear==NULL) front=NULL;
             qent--;
      }
      else
      printf("\n delQueue Empty");
}

void delQueuenofree()
{
      struct Node *temp, *var=rear;
      if(var==rear)
      {
             rear = rear->next;
             //free(var);
             if (rear==NULL) front=NULL;
             qent--;
      }
      else
      printf("\n delQueue Empty");
}

void pushn(struct work *twork)
{
     struct Node *temp;
     temp=(struct Node *)malloc(sizeof(struct Node));
     qent++;
     bcopy (twork,&temp->swork,sizeof(struct work));
     temp->swork.freeme=temp;
     if (front == NULL)
     {
           front=temp;
           front->next=NULL;
           rear=front;
     }
     else
     {
           front->next=temp;
           front=temp;
           front->next=NULL;
     }
}


// two-phase version only holds the lock during queue-manip

// part1 can be done without the lock.
// returns the to-be-queued copy of <twork>
struct work* pushn2_part1(struct work *twork)
{
     struct Node *temp;
     temp=(struct Node *)malloc(sizeof(struct Node));
     bcopy (twork,&temp->swork,sizeof(struct work));
     temp->swork.freeme=temp;
     return &temp->swork;
}

// caller needs a lock for this part.
// call with element returned from part1().
void pushn2_part2(struct work *twork)
{
     struct Node* temp = (struct Node*)twork->freeme;
     qent++;
     if (front == NULL)
     {
           front=temp;
           front->next=NULL;
           rear=front;
     }
     else
     {
           front->next=temp;
           front=temp;
           front->next=NULL;
     }
}


void display()
{
     struct Node *var=rear;
     if(var!=NULL)
     {
           printf("\nElements are as:  ");
           while(var!=NULL)
           {
                printf("\t%s",var->swork.name);
                var=var->next;
           }
     printf("\n");
     } 
     else
     printf("\ndisplayQueue is Empty");
}

void displaycurrent()
{
     struct Node *var=rear;
     if(var!=NULL)
     {
        printf("current: \t%s",var->swork.name);
        printf("\n");
     } 
     else
        printf("\ndisplaycurrentQueue is Empty");
}

void displayqent()
{
     printf("qent = %d\n",qent);
}

int addrqent()
{
      return qent;
}

struct work * addrcurrents()
{
     struct Node *var=rear;
     if(var!=NULL)
           return &var->swork;
     else
        printf("\naddrcurrentsQueue is Empty");
     return NULL;
}

