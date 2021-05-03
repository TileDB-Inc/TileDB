The blosc files here were copied from release 1.21.0 of the `c-blosc` repository
  of the Blosc project.
All we are using here is the byte shuffle code, so we do not have the library itself
  as an external dependency.
We only use a selected subset of the sources.
With one exception, these files are used unmodified.
See `README-TRADEOFF.md` for the rationale behind modifying any of the sources.

* https://github.com/Blosc/c-blosc
* https://github.com/Blosc/c-blosc/releases/tag/v1.21.0