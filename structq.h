#ifndef STRUCTQ_H
#define STRUCTQ_H


#include "bf.h"

void delQueue();

void delQueuenofree();

void pushn(struct work *twork);

struct work* pushn2_part1(struct work *twork);
void         pushn2_part2(struct work *twork);

void display();

void displaycurrent();

void displayqent();

int  addrqent();

struct work * addrcurrents();



#endif
