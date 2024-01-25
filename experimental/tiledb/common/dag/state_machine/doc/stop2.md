Case I: Assuming only the producer node can issue a stop, and that only as provided by
the user-defined function at a well-defined location.

The basic logic of a producer node is:
```C
   tmp = f()
   inject(tmp)
   source_fill()
   source_push()
```


Now we include a mechanism so that user-supplied function f can indicate it's time to stop.
```C
  bool token = false;
  auto tmp = f(token);
  if (token == stop) {
    port_exhausted()
    return
  }
  source_fill()
  source_push()
```

Expanded and with proof outline
```C
  init: { state = 00 ∧ stop = 0 }

  while (not done)

     /* { state = 00 ∨ state = 01 } ∧ { stop = 0 } */
     bool token = false;
     auto tmp = f(token);

     /* { state = 00 ∨ state = 01 } ∧ { stop = 0 } */

     if (token == stop) {

       /* { state = 00 ∨ state = 01 } ∧ { stop = 0 } */
       event(stop):

         /* { state = 00 ∨ state = 01 } ∧ { stop = 0 } */
         exit action: (none)

         /* { state = 00 ∨ state = 01 } ∧ { stop = 0 } */
         state transition: { stop = 0 } → { stop = 1 }

         /* { state = 00 ∨ state = 01 } ∧ { stop = 1 } */
         entry action: { state = 00 } → term_source
                       { state = 01 } → term_source

         /* { state = 00 ∨ state = 01 } ∧ { stop = 1 } */
     }

     event(source_fill):
       
     event(source_push):
```


The basic logic of a consumer node is:
```C
   sink_pull()
   tmp = extract()
   f(tmp)
   sink_drain()
```



Note that with respect to the sink, a stop event can happen at any time.  If there is
still data in the pipeline, when there is a stop, we want it to continue to flow to
the sink until everything has been transferred and handled, i.e., until the state is
00.  Basically, this means that everything continues in the same way regardless of
whether we are in a stopping state -- except when { state == 00 } ∧ { stop = 1 } on
the sink_pull event.  In that case, rather than waiting, we just continue, transition
to { state = done }, and then invoke the term_sink action.

```C
  init: { state = 00 } ∧ { stop = 0 }

  while (not done)

     /* { state = 00 ∨ state = 01 ∨ state = 10 ∨ state = 11 } ∧ { stop = 0 ∨ stop = 1 }
     event(sink_pull):

       /* { state = 00 ∨ state = 01 ∨ state = 10 ∨ state = 11 } ∧ { stop = 0 ∨ stop = 1 }
       exit action: { state = 00 }              ∧ { stop = 0 }            → sink_wait
                    { state = 00 }              ∧ { stop = 1 }            → none
                    { state = 01 ∨ state = 11 } ∧ { stop = 0 ∨ stop = 1 } → none
                    { state = 10 }              ∧ { stop = 0 ∨ stop = 1 } → sink_swap

       state transition: { state = 00 }              ∧ { stop = 0 }            → n/a (wait)
                         { state = 00 }              ∧ { stop = 1 }            → { state = done }
                         { state = 01 ∨ state = 10 } ∧ { stop = 0 ∨ stop = 1 } → { state = 01 }   ∧ { stop = 0 ∨ stop = 1 }
                         { state = 11 }              ∧ { stop = 0 ∨ stop = 1 } → { state = 11 }   ∧ { stop = 0 ∨ stop = 1 }

       entry action: { state = done }                                                   → term_sink
                     { state = 10 }                           ∧ { stop = 0 ∨ stop = 1 } → sink_swap
                     { state = 00 ∨ state = 01 ∨ state = 11 } ∧ { stop = 0 ∨ stop = 1 } → none

       /* { state = 01 ∨ state = 11 } ∧ { stop = 0 }

     /* { state = 01 ∨ state = 11 } ∧ { stop = 0 ∨ stop = 1 }
     auto tmp = extract()

     /* { state = 01 ∨ state = 11 } ∧ { stop = 0 ∨ stop = 1 }
     f(tmp)

     /* { state = 01 ∨ state = 11 } ∧ { stop = 0 ∨ stop = 1 }
     event(sink_drain):

     /* { state = 00 ∨ state = 10 } ∧ { stop = 0 ∨ stop = 1 }
```
Here, we are essentially processing the stop in the sink_pull event.

An alternative would be to do it in sink_drain, as follows:

```C
  init: { state = 00 } ∧ { stop = 0 }

  while (not done)

     /* { state = 00 ∨ state = 01 ∨ state = 10 ∨ state = 11 } ∧ { stop = 0 ∨ stop = 1 }
     event(sink_pull):

     /* { state = 01 ∨ state = 11 } ∧ { stop = 0 ∨ stop = 1 }
     auto tmp = extract()

     /* { state = 01 ∨ state = 11 } ∧ { stop = 0 ∨ stop = 1 }
     f(tmp)

     /* { state = 01 ∨ state = 11 } ∧ { stop = 0 ∨ stop = 1 }
     event(sink_drain):

       /* { state = 01 ∨ state = 11 } ∧ { stop = 0 ∨ stop = 1 }
       exit action: (none) 

       /* { state = 01 ∨ state = 11 } ∧ { stop = 0 ∨ stop = 1 }
       state transition: { state = 01 } ∧ { stop = 0 }            → { state = 00 } ∧ { stop = 0 }
                         { state = 01 } ∧ { stop = 1 }            → { state = done }
                         { state = 11 } ∧ { stop = 0 ∨ stop = 1 } → { state = 10 } ∧ { stop = 0 ∨ stop = 1 }

       entry action: { state = 00 ∨ state = 10 } ∧ { stop = 0 } → notify_source
                     { state = 10 }              ∧ { stop = 1 } → none
                     { state = done }                           → term_sink


       /* { state = 01 ∨ state = 11 } ∧ { stop = 0 }

     /* { state = 00 ∨ state = 10 } ∧ { stop = 0 ∨ stop = 1 }
```
These are essentially equivalent -- we've just moved the code "back" from the sink_pull to the sink_drain case.
However, for reasons of symmetry, restricting scheduler-type actions to push and pull
and state updates to fill and drain seems to make sense.
