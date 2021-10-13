> In order to add a new function in c++ api (e.g. call the function in examples like fragment_info.cc), we have to first add the function in /sm/cpp_api.

* Regarding `fragment_info.fragment_num()` in **fragment_info.cc** as an example:

1. The object **fragment_info** is from class **/sm/cpp_api/fragment_info.h**. 

2. In **/sm/cpp_api/fragment_info.h**, the constructor has 1) the ctx; 2) object `fragment_info` which is a pointer pointing to class `tiledb_fragment_info_t`;

`tiledb_fragment_info_t` is a struct that only contains a pointer to `/sm/fragment/fragment_info.h`.

 3) variable `fragment_info_` which is crated as a shared_ptr overwriting `fragment_info`.

3. When calling `fragment_info.fragment_num()`, the `ctx.handle_error()` is the wrapper like `catch{}`. Inside the wrapper, what is really called is `tiledb_fragment_info_get_fragment_num()`.

4. `tiledb_fragment_info_get_fragment_num()` is inside **tiledb.cc**. It has 3 parameters: 1) ctx; 2) A pointer to class **tiledb_fragment_info_t** called *fragment_info*; 3) A pointer to unit32_t which stores the result.

5. The result is fragment_info-> (object of `tiledb_fragment_info_t`, which has a pointer to `/sm/fragment/fragment_info.h`) -> fragment_num (a public attribute of `fragment_info.h`)


