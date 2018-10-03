# coep: Cluster Optimization with Embarrassing Parallelism

Useful when parameter estimation is needed over a very expensive function call
that itself has embarassingly parallel properties (e.g., running a stochastic
function a large number of times, running a function over a large number of
examples, etc.), and you have access to a large number of cores to spread the
function over.

Works by a quasi-producer/consumer model where the function is set up across a
number of processes so that the master optimization loop can farm out the
computations when running the function.

## Setup

TO WRITE
