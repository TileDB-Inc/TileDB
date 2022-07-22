

The file fsm.h implements a state machine for two communicating ports, `Source` and
`Sink`.  Each port has two states, empty or full.  There are two transition events
associated with the source: source_fill, which transitions from empty to full and
tells the state machine there is an item in the `Source`, and and source_push, which
initiates transfer to the `Sink` and transitions from full to empty.

Similarly, there are two events associated with the sink: sink_drain, which
transitions from full to empty and tells the `Sink` node that its item has been
removed, and and sink_pull, which attempts to transfer an item from the `Source` and
transitions from empty to full. For simplicity, though we may need them in the future,
there are currently no defined events for startup, stop, forced shutdown, or abort.

To complete the functionality of the state machine for the purposes of safely
transferring data from a `Source` to a `Sink`, there are exit and entry actions
associated with selected states and events.

Diagrams for the source and sink state machines can be found in
source_state_machine.svg and sink_state_machine.svg, respectively.  

The following diagrams show the entry and exit actions for each state machine, for the
`Source` and `Sink` in isolation.  Note that there is some "spooky action at a
distance" between the two due to waiting in one with notification from the other.
We don't show those interactions in the diagram, but capture than in the state
transition and event action tables.

<img source="source_state_machine.svg" width="360" />
<img source="sink_state_machine.svg" width="360" />

Our basic goal for the `Source` and `Sink` ports is to transfer a data item from a
`Source` to a connected (bound) `Sink`.  At a high level, the way a client would use
the `Source` to do this is the following:
- create a data item
- insert the data item into a `Source` port
- invoke the source_fill event
- invoke the source_push event


Similarly, the desired usage of a `Sink` port is also to transfer a data item from a
`Source` to a bound `Sink`.  At a high level, the way a client would use the `Source`
is the following
- invoke the sink_pull event
- extract the data item from the `Sink` port
- consume the item
- invoke the sink_drain event

Since the `Source` and `Sink` are bound together, we need to consider a product state
machine, one which has all combinations of `Source` and `Sink` states.  Since the
`Source` and `Sink` states each have *empty* and *full* states, we denote the product
states as *empty_empty*, *empty_full*, *full_empty*, and *full_full*, which represent
*sourcestate_sinkstate*.

Based on these product states and the four above events, the state transition table
for the product state machine (which we will just refer to as the "state machine"
below is the following:

|  State          |||| Event      |||
|--------|--------|-------------|-------------|-------------|-------------|----------|
| Source |  Sink  | source_fill | source_push | sink_drain  | sink_pull   | shutdown |
| empty  | empty  | full_empty  | empty_empty |             | empty_full  |          |
| empty  | full   | full_full   | empty_full  | empty_empty | empty_full  |          |
| full   | empty  |             | empty_full  |             | empty_full  |          |
| full   | full   |             | empty_full  | full_empty  | full_full   |          |  


Using this table, we can include the states as predicates with "proof outline" 
statements for the `Source` operation:
```C
   while (not done) {
     /* { state = empty_empty ∨ state = empty_full } ∧ { source_item = empty } */
     do produce and insert item
     /* { state = empty_empty ∨ state = empty_full } ∧ { source_item = full } */
     do source_fill
     /* { state = full_empty ∨ state = full_full } ∧ { source_item = full } */
     do source_push
     /* { state = empty_empty ∨ state = empty_full } ∧ { source_item = empty } */
   }
```

Similarly for `Sink`:
```C
   while (not done) {
     /* { state = empty_empty ∨ state = full_empty } ∧ { sink_item = empty } */
     do sink_pull
     /* { state = empty_full ∨ state = full_full } ∧ { sink_item = full } */
     do extract and consume item  
     /* { state = empty_full ∨ state = full_full } ∧ { sink_item = full } */
     do sink_drain
     /* { state = empty_empty ∨ state = full_empty } ∧ { sink_item = empty } */
   }
```

Now, the `Source` and `Sink` need to coordinate `source_push` and `sink_pull` so there
is not a race condition (nor a deadlock) when making transitions in the state machine.
Moreover, we have to make sensible transitions.  That is, we only be able to
succesfully do a source_push when the `Sink` state is empty (and the `Sink` item
itself is empty).  This is why we insert a new item and *then* invoke source_fill.
Until the state has transitioned to indicate the state of the `Source` is full, the
`Sink` will not attempt to transfer the item.  Similarly, we empty the sink_item and
*then* signal that the `Sink` is in the empty state.

To do this, we associate exit and entry actions with each state transition, some of
which will synchronize between `Source` and `Sink`.  These actions are used with the
state transition thusly:

  - begin_transition: given old_state and event
  - execute exit(old_state, event)
  - new_state = transition(old_state, event)
  - execute entry(new_state, event)

Note that the exit action is called *before* the state transition.
Note also that the entry action is called with the new state
(the post transition state).

The tables for exit actions to be perfomed on state transitions is:

   |      States     ||||                     Events                                  |||
   |--------|--------|-------------|-------------|-------------|-------------|----------|
   | Source |  Sink  | source_fill | source_push | sink_drain  | sink_pull   | shutdown |
   | empty  | empty  |             |             |             | sink_wait   |          |
   | empty  | full   |             |             |             |             |          |
   | full   | empty  |             | source_swap |             | sink_swap   |          |
   | full   | full   |             | source_wait |             |             |          |


The table for entry actions to be performend on state transitions is:


   |      States     ||||                     Events                                  |||
   |--------|--------|-------------|-------------|---------------|-------------|----------|
   | Source |  Sink  | source_fill | source_push | sink_drain    | sink_pull   | shutdown |
   | empty  | empty  |             |             | notify_source |             |          |
   | empty  | full   |             |             |               |             |          |
   | full   | empty  | notify_sink | source_swap | notify_source | sink_swap   |          |
   | full   | full   | notify_sink |             |               |             |          |


The `source_swap` function is used to potentially transfer the data items associated with
`Source` and `Sink` from the `Source` to the `Sink` (as well as changing the state if
data transfer is carried out).  The `source_swap` function is invoked whenever the state is 
full_empty (which is when there is an item in `Source` and space available to transfer to
the `Sink`.
The data transfer is carried out by swapping the
`Source` and `Sink` items and changing the state of full_empty to empty_full.

When the state is empty_empty, the `Sink` will wait for the `Source` to become full.  The
`Source` will notify the `Sink` when it becomes full.  Similarly, if the state is full_full,
the `Source` will wait until it is signalled by the `Sink` that the `Sink` is empty.

In more detail, we can describe the `Source` behavior (including proof outline predicates);
```C
  init: { state = empty_empty ∧ source_item = empty }
  while (not done)

     /* { state = empty_empty ∨ state = empty_full } ∧ { source_item = empty } */
     client of the source inserts an item  /* Note that although the Sink can execute and potentially change the
                                              state here, the allowable transitions do not end up changing it */

     /* { state = empty_empty ∨ state = empty_full } ∧ { source_item = full } */
     client invokes source_fill event to transition from empty to full.

     /* state machine locks mutex */
     state machine invokes exit action
     if { state = empty_empty ∨ state = empty_full } → none
     /* { state = empty_empty ∨ state = empty_full } ∧ { source_item = full } */
     state machine performs transition
       { state = empty_empty } → { state = full_empty } ∧ { source_item = full } */
       { state = empty_full } → { state = full_full } ∧ { source_item = full } */
     /* { state = full_empty ∨ state = full_full } ∧ { source_item = full } */
     Source notifies Sink that it is full
     /* { state = full_empty ∨ state = full_full } ∧ { source_item = full } */
     Source returns
     /* state machine unlocks mutex */

     /* Before the Source begins the source_push, the Sink may pull, drain, do both, or do nothing */

     /* { state = full_empty ∨ state = full_full ∨ state = empty_full ∨ state = empty_empty } ∧ { source_item = empty ∨ source_item = full } */
     client invokes source_push event
     state machine locks the mutex

     /* { state = full_empty ∨ state = full_full ∨ state = empty_full ∨ state = empty_empty } ∧ { source_item = empty ∨ source_item = full } */
     state machine executes source_push exit action, which may be one of the following, depending on the state */
       if { state = empty_empty ∨ state = empty_full } → none
       if state = full_empty → execute source_swap
       if state = full_full → execute source_wait
         /* pre_source_swap: { state = full_empty } ∧ { source_item = full } */
            state machine swaps source_item and sink_item -- swap does not change state
         /* post_source_swap: { state = full_empty } ∧ { source_item = empty } */
       if { state = full_full } → execute source_wait  /* wait for Sink to become empty */
          /* Important! When the state machine comes back from wait, it is now no longer in the state it was when it started the wait. */
          The Sink could leave the state as { state = empty_empty ∨ state = empty_full ∨ state = full_empty } before Source starts to run
            /* { state = empty_empty ∨ state = empty_full ∨ state = full_empty } */
            Since notification happens in entry portion, the next_state may be { next_state = empty_empty ∨ next_state = full_empty ∨ next_state = empty_full }

     make state transition according to state transition table and next_state set by most recent event
       { state = empty_empty ∨ state = empty_full ∨ state = full_empty }

     state machine invokes entry action
       if { state = empty_empty } → none
       if { state = empty_full } → none
       if { state = full_empty } → swap and change state to empty_full
         /* post_entry: { state = empty_empty ∨ state = empty_full } ∧ { source_item = empty } */
     state machine unlocks mutex

      /* post_push: { state = empty_empty ∨ state = empty_full } ∧ { source_item = empty } */
    /* end_loop: { state = empty_empty ∨ state = empty_full } ∧ { source_item = empty } */
  /* post_loop: { state = empty_empty ∨ state = empty_full } ∧ { source_item = empty } */
```

The `Sink` is the dual of the Source.  Note that we start with sink_pull.
We can describe the `Sink` behavior (including proof outline predicates):
```C
  init: { state = empty_empty ∧ sink_item = empty }
  while (not done)
     /* { state = empty_empty ∨ state = empty_full } ∧ { sink_item = empty ∨ sink_item = full } */
     /* Before client invokes the sink_pull event, the source could have filled, filled and pushed, or done nothing */
       if source filled: empty_empty → full_empty
       if source filled and pushed empty_empty → empty_full
       if source filled and pushed and filled empty_empty → full_full
       if source did nothing state does not change
     /* { state = empty_empty ∨ state = empty_full ∨ state = full_empty ∨ state = full_empty } ∧ { sink_item = empty ∨ sink_item = full } */
     client invokes sink_pull event
     state machine locks mutex

     /* { state = empty_empty ∨ state = empty_full ∨ state = full_empty ∨ state = full_full } ∧ { sink_item = empty ∨ sink_item = full } */
     state machine invokes sink_pull exit action, which may be one of the following, depending on the state
       { state = empty_full ∨ state = full_full } → none
       { state = full_empty } → sink_swap 
       { state = empty_empty } → sink_wait
         /* pre_sink_swap: { state = full_empty } ∧ { sink_item = empty }
         /* post_sink_swap: { state = full_empty } ∧ { sink_item = full }

         /* pre_sink_wait: { state = empty_empty } /* wait for source to become full */
         /* Important! The state machine is now no longer in the state it was when it started the wait. */
         The Source could leave the state as { state = empty_full ∨ state = full_empty ∨ state = full_full } before Sink starts to run
            /* { state = empty_full ∨ state = full_empty ∨ state = full_full } */
            Since notification happens in entry portion, the next_state may be { next_state = empty_full ∨ next_state = full_empty ∨ next_state = full_full }

       make state transition according to state transition table and state and next_state set by most recent event
       /* { state = empty_full ∨ state = full_empty ∨ state = full_full } ∧ { sink_item = full } */

       /* { state = empty_full ∨ state = full_full } ∧ { sink_item = full } */
       state machine invokes entry action 
         if { state = full_full } → none
         if { state = empty_full } → none
	 if { state = full_empty } → swap and  change state to empty_full
       { state = empty_full ∨ state = full_full } ∧ { sink_item = full } */
       state machine returns
     { state = empty_full ∨ state = full_full }
     state machine unlocks mutex
     /* post_pull: { state = empty_full ∨ state = full_full } ∧ { sink_item = full } */

     client of the sink extracts the item  /* Note that although the Source can execute and potentially change the
                                                state here, the allowable transitions do not end up changing it */
     /* { state = empty_full ∨ state = full_full } ∧ { sink_item = empty } */
     client invokes sink_drain event to transition from full to empty 
     state machine locks mutex
     /* { state = empty_full ∨ state = full_full } ∧ { sink_item = empty } */
     state machine performs exit action
     if { state = empty_full ∨ state = full_full } → none
     { state = empty_full ∨ state = full_full }
     state machine performs transition
     { state = empty_full } → { state = empty_empty }
     { state = full_full } → { state = full_empty }
     /* { state = empty_empty ∨ state = full_empty } ∧ { sink_item = empty } */
     state machine performs entry action
       { state = empty_empty } → notify_source
       { state = full_empty } → notify_source
     Sink returns
     state machine unlocks mutex
     /* end_loop: { state = empty_empty ∨ state = full_empty } ∧ { sink_item = empty } */
  /* post_loop: { state = empty_empty ∨ state = full_empty } ∧ { sink_item = empty } */
```

Operations carried out directly by the state machine are protected by a lock.  When
the `Source` or `Sink` wait, they do so on a condition variable using that same lock.

Since the synchronization implies that if the `Source` and `Sink` execute the same
number of loops, the `Source` will have { state = empty_empty ∨ state = empty_full }
while the `Sink` will have { state = empty_empty ∨ state = full_empty }.
The final state of the state machine must be 
{ state = empty_empty ∨ state = empty_full } ∧ { state = empty_empty ∨ state = full_empty } ∧ { item = empty },
i.e., { state = empty_empty } ∧ { item = empty }.

**NB:** The sink_swap and source_swap functions are identical. Each checks to see if
the state is equal to full_empty, if so, swap the state to empty_full (and perform an
action swap of the items assoiated with the source and sink), and notifies the other.
If the state is not equal to full_empty, the swap function notifies the other and goes
into a wait.

Thus, we may not need separate swaps for `Source` and `Sink`, nor separate condition
variables, nor separate notification functions.  I have verified that this works
experimentally, but I am leaving things separate for now.

