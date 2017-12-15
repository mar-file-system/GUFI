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

