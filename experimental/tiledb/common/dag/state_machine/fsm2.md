

The file fsm.h implements a state machine for two communicating ports, `Source` and
`Sink`.  Each port has two states, empty or full.  There are two transition events
associated with the source: fill, which transitions from empty to full and
tells the state machine there is an item in the `Source`, and and push, which
initiates transfer to the `Sink` and transitions from full to empty.

Similarly, there are two events associated with the sink: drain, which
transitions from full to empty and tells the `Sink` node that its item has been
removed, and and pull, which attempts to transfer an item from the `Source` and
transitions from empty to full. For simplicity, though we may need them in the future,
there are currently no defined events for startup, stop, forced shutdown, or abort.

To complete the functionality of the state machine for the purposes of safely
transferring data from a `Source` to a `Sink`, there are exit and entry actions
associated with selected states and events.

The following diagrams show the entry and exit actions for each state machine, for the
`Source` and `Sink` in isolation.  Note that there is some "spooky action at a
distance" between the two due to waiting in one with notification from the other.
We don't show those interactions in the diagram, but capture than in the state
transition and event action tables.

<img source="source_state_machine.svg" width="360" />
<img source="sink_state_machine.svg" width="360" />


We can combine the source and sink state machines into a single "product" state
machine wherein the overall state is represented with two bits, one for the source and
one for the sink, e.g., "00" meaning that the source state is 0 and the sink state is
1.  The following diagram shows the state transitions for the product state machine.

<img source="two_stage.svg" width="720" />

One particular aspect of this is the pull event from state 00 and the push event from
state 11.  In these cases, the exit action is "wait", since the only valid state to
complete a push or a pull is from state 01. Accordingly, we perform notifications as
entry actions to state 10.  

When a source or sink thread is released from its wait, it is still in the push or
pull event.  To enable it to complete that desired operation, we restart the event
processing for the push or pull event in the current state.

(As a slight optimization, we also perform notifications on entry to states 00 and 11.
The waiter doesn't get to move in that case, but does get to leave the wait and
produce or consume an item and then try to push or pull again.)


Our basic goal for the `Source` and `Sink` ports is to transfer a data item from a
`Source` to a connected (bound) `Sink`.  At a high level, the way a client would use
the `Source` to do this is the following:
- create a data item
- insert the data item into a `Source` port
- invoke the fill event
- invoke the push event


Similarly, the desired usage of a `Sink` port is also to transfer a data item from a
`Source` to a bound `Sink`.  At a high level, the way a client would use the `Source`
is the following
- invoke the pull event
- extract the data item from the `Sink` port
- consume the item
- invoke the drain event



Based on these product states and the four above events, the state transition table
for the product state machine (which we will just refer to as the "state machine"
below is the following:

|                 |||| Event      |||
|--------|-------------|-------------|-------------|-------------|----------|
| State  | fill        | push        | drain       | pull        | stop     |
| 00     | 10          | 00          |             | 01          |          |
| 01     | 11          | 01          | 00          | 01          |          |
| 10     |             | 01          |             | 01          |          |
| 11     |             | 01          | 10          | 11          |          |  


Using this table, we can include the states as predicates with "proof outline" 
statements for the `Source` operation:
```C
   while (not done) {
     /* { state = 00 ∨ state = 01 } ∧ { source_item = empty } */
     do produce and insert item
     /* { state = 00 ∨ state = 01 } ∧ { source_item = full } */
     do fill
     /* { state = 10 ∨ state = 11 } ∧ { source_item = full } */
     do push
     /* { state = 00 ∨ state = 01 } ∧ { source_item = empty } */
   }
```

Similarly for `Sink`:
```C
   while (not done) {
     /* { state = 00 ∨ state = 10 } ∧ { sink_item = empty } */
     do pull
     /* { state = 01 ∨ state = 11 } ∧ { sink_item = full } */
     do extract and consume item  
     /* { state = 01 ∨ state = 11 } ∧ { sink_item = full } */
     do drain
     /* { state = 00 ∨ state = 10 } ∧ { sink_item = empty } */
   }
```

Now, the `Source` and `Sink` need to coordinate `push` and `pull` so there
is not a race condition (nor a deadlock) when making transitions in the state machine.
Moreover, we have to make sensible transitions.  That is, we only be able to
succesfully do a push when the `Sink` state is empty (and the `Sink` item
itself is empty).  This is why we insert a new item and *then* invoke fill.
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

   |       ||||                     Events                                  |||
   |--------|-------------|-------------|-------------|-------------|----------|
   | State  | fill        | push        | drain       | pull        | stop     |
   | 00     |             | return      |             | sink_wait   |          |
   | 01     |             | return      |             | return      |          |
   | 10     |             | source_swap |             | sink_swap   |          |
   | 11     |             | source_wait |             | return      |          |


The table for entry actions to be performend on state transitions is:


   |           ||||                     Events                                  |||
   |--------|-------------|-------------|---------------|-------------|----------|
   | State  | fill        | push        | drain         | pull        | shutdown |
   | 00     |             |             | notify_source |             |          |
   | 01     |             |             |               |             |          |
   | 10     | notify_sink |             | notify_source |             |          |
   | 11     | notify_sink |             |               |             |          |


The `source_swap` function is used to potentially transfer the data items associated with
`Source` and `Sink` from the `Source` to the `Sink` (as well as changing the state if
data transfer is carried out).  The `source_swap` function is invoked whenever the state is 
10 (which is when there is an item in `Source` and space available to transfer to
the `Sink`.
The data transfer is carried out by swapping the
`Source` and `Sink` items and changing the state of 10 to 01.

When the state is 00, the `Sink` will wait for the `Source` to become full.  The
`Source` will notify the `Sink` when it becomes full.  Similarly, if the state is 11,
the `Source` will wait until it is signalled by the `Sink` that the `Sink` is empty.

In more detail, we can describe the `Source` behavior (including proof outline predicates);
```C
  init: { state = 00 ∧ source_item = empty }
  while (not done)

     /* { state = 00 ∨ state = 01 } ∧ { source_item = empty } */
     client of the source inserts an item  /* Note that although the Sink can execute and potentially change the
                                              state here, the allowable transitions do not end up changing it */

     /* { state = 00 ∨ state = 01 } ∧ { source_item = full } */
     client invokes fill event to transition from empty to full.

     /* state machine locks mutex */
     state machine invokes exit action
     if { state = 00 ∨ state = 01 } → none
     /* { state = 00 ∨ state = 01 } ∧ { source_item = full } */
     state machine performs transition
       { state = 00 } → { state = 10 } ∧ { source_item = full } */
       { state = 01 } → { state = 11 } ∧ { source_item = full } */
     /* { state = 10 ∨ state = 11 } ∧ { source_item = full } */
     Source notifies Sink that it is full
     /* { state = 10 ∨ state = 11 } ∧ { source_item = full } */
     Source returns
     /* state machine unlocks mutex */

     /* Before the Source begins the push, the Sink may pull, drain, do both, or do nothing */

     /* { state = 10 ∨ state = 11 ∨ state = 01 ∨ state = 00 } ∧ { source_item = empty ∨ source_item = full } */
     client invokes push event
     state machine locks the mutex

     /* { state = 10 ∨ state = 11 ∨ state = 01 ∨ state = 00 } ∧ { source_item = empty ∨ source_item = full } */
     state machine executes push exit action, which may be one of the following, depending on the state */
     restart:
       if { state = 00 ∨ state = 01 } → none
       if state = 10 → execute source_swap
       if state = 11 → execute source_wait
         /* pre_source_swap: { state = 10 } ∧ { source_item = full } */
            state machine swaps source_item and sink_item -- swap does not change state
         /* post_source_swap: { state = 10 } ∧ { source_item = empty } */
       if { state = 11 } → execute source_wait  /* wait for Sink to become empty */
          /* Important! When the state machine comes back from wait, it is now no longer in the state it was when it started the wait. */
          /* We therefore restart event processing for the push event, given the state present when coming back from wait: goto restart.*/

        make state transition according to state transition table and next_state set by most recent event
          { state = 00 } → { state = 00 }
          { state = 01 ∨ state = 10 } → { state = 01 }

       { state = 00 ∨ state = 01 } ∧ { source_item = empty } */
       state machine invokes entry action (none) 

       /* post_entry: { state = 00 ∨ state = 01 } ∧ { source_item = empty } */
       state machine unlocks mutex

      /* post_push: { state = 00 ∨ state = 01 } ∧ { source_item = empty } */
    /* end_loop: { state = 00 ∨ state = 01 } ∧ { source_item = empty } */
  /* post_loop: { state = 00 ∨ state = 01 } ∧ { source_item = empty } */
```

The `Sink` is the dual of the Source.  Note that we start with pull.
We can describe the `Sink` behavior (including proof outline predicates):
```C
  init: { state = 00 ∧ sink_item = empty }
  while (not done)
     /* { state = 00 ∨ state = 01 } ∧ { sink_item = empty ∨ sink_item = full } */
     /* Before client invokes the pull event, the source could have filled, filled and pushed, or done nothing */
       if source filled: 00 → 10
       if source filled and pushed 00 → 01
       if source filled and pushed and filled 00 → 11
       if source did nothing state does not change
     /* { state = 00 ∨ state = 01 ∨ state = 10 ∨ state = 10 } ∧ { sink_item = empty ∨ sink_item = full } */
     client invokes pull event
     state machine locks mutex

     /* { state = 00 ∨ state = 01 ∨ state = 10 ∨ state = 11 } ∧ { sink_item = empty ∨ sink_item = full } */
     state machine executes pull exit action, which may be one of the following, depending on the state
     restart:
       { state = 01 ∨ state = 11 } → none
       { state = 10 } → sink_swap 
       { state = 00 } → sink_wait
         /* pre_sink_swap: { state = 10 } ∧ { sink_item = empty }
         /* post_sink_swap: { state = 10 } ∧ { sink_item = full }

         /* pre_sink_wait: { state = 00 } /* wait for source to become full */
          /* Important! When the state machine comes back from wait, it is now no longer in the state it was when it started the wait. */
          /* We therefore restart event processing for the pull event, given the state present when coming back from wait: goto restart.*/

       /* { state = 01 ∨ state = 10 ∨ state = 11 } ∧ { sink_item = full } */      
       make state transition according to state transition table and state and next_state set by most recent event
         { state = 01 ∨ state = 10 } → { state = 01 }
         { state = 11 } → { state = 11 }

       /* { state = 01 ∨ state = 11 } ∧ { sink_item = full } */
       state machine invokes pull entry action (none)
       /* post_entry: { state = 01 ∨ state = 11 } ∧ { sink_item = full } */ 
     state machine unlocks mutex
     /* post_pull: { state = 01 ∨ state = 11 } ∧ { sink_item = full } */

     client of the sink extracts the item  /* Note that although the Source can execute and potentially change the
                                                state here, the allowable transitions do not end up changing it */
     /* { state = 01 ∨ state = 11 } ∧ { sink_item = empty } */
     client invokes drain event to transition from full to empty 
     state machine locks mutex
     /* { state = 01 ∨ state = 11 } ∧ { sink_item = empty } */
     state machine performs exit action
     if { state = 01 ∨ state = 11 } → none
     { state = 01 ∨ state = 11 }
     state machine performs transition
     { state = 01 } → { state = 00 }
     { state = 11 } → { state = 10 }
     /* { state = 00 ∨ state = 10 } ∧ { sink_item = empty } */
     state machine performs entry action
       { state = 00 } → notify_source
       { state = 10 } → notify_source
     Sink returns
     state machine unlocks mutex
     /* end_loop: { state = 00 ∨ state = 10 } ∧ { sink_item = empty } */
  /* post_loop: { state = 00 ∨ state = 10 } ∧ { sink_item = empty } */
```

Operations carried out directly by the state machine are protected by a lock.  When
the `Source` or `Sink` wait, they do so on a condition variable using that same lock.

Since the synchronization implies that if the `Source` and `Sink` execute the same
number of loops, the `Source` will have { state = 00 ∨ state = 01 }
while the `Sink` will have { state = 00 ∨ state = 10 }.
The final state of the state machine must be 
{ state = 00 ∨ state = 01 } ∧ { state = 00 ∨ state = 10 } ∧ { item = empty },
i.e., { state = 00 } ∧ { item = empty }.

**NB:** The sink_swap and source_swap functions are identical. Each checks to see if
the state is equal to 10, if so, swap the state to 01 (and perform an
action swap of the items assoiated with the source and sink), and notifies the other.
If the state is not equal to 10, the swap function notifies the other and goes
into a wait.

Thus, we may not need separate swaps for `Source` and `Sink`, nor separate condition
variables, nor separate notification functions.  I have verified that this works
experimentally, but I am leaving things separate for now.

