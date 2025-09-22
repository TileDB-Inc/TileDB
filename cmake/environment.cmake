#
# cmake/environment.cmake
#
# The MIT License
#
# Copyright (c) 2022 TileDB, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

# -------------------------------------------------------
# Usage
# -------------------------------------------------------
#
# ```
# include(environment)
# ```
#
# CAUTION: This module defines "global" variables for a facility. Because CMake
# has no actual global scope, this module should be included in the root
# CMakeLists.txt files, as the top-level directory scope is the closest thing
# CMake has to a global scope.

# -------------------------------------------------------
# Preliminaries
# -------------------------------------------------------
include_guard()
#
# Module-scope log level
#
set(TileDB_Environment_Log_Level VERBOSE)
message(${TileDB_Environment_Log_Level} ">>> environment.cmake begin >>>")

# -------------------------------------------------------
# Environments for CMake
# -------------------------------------------------------
#
# An environment is a context that provides defaults and abbreviations for
# common use cases. The name "environment" is drawn from LaTeX, which builds
# upon the "raw" macro language Tex to create something closer to block-
# structured programming. Environments can replace boilerplate-heavy code where
# there is sufficient variation that the resulting code cannot be readily
# generated from the arguments to a function call.
#
# Environments themselves provide a small number of services
# - Syntax matching for commence/conclude.
#     This facility ensures the integrity of pairing of "commence" and
#     "conclude". Environments may be nested.
# - Properties on the environment.
#     This provides a reliable way of storing environment-specific data. Such
#     data does not persist past the end of the environment, but it can be
#     accessed during the post-processing step.
# - A unique prefix property.
#     A unique prefix affords most of the benefits of a namespace.
#
# Environments in CMake share many of the limitations of CMake itself:
# 1. The environment does not create a new scope, so there's no concept of local
#    variables inside the environment. This is not desirable, but it's necessary
#    because CMake has no way of making a scope other than directory or function
#    scopes.
# 2. There's no way to reuse names of existing functions to provide alternate
#    versions with environment-provided arguments. Instead, there are new
#    environment-aware functions that do so.
# 3. CMake has no namespacing or aliasing facility but only a single, global
#    namespace. It is too risky to use function names "begin" and "end" in light
#    of future CMake-defined functions. Thus we use the adequate but partly-
#    surprising names "commence" and "conclude".
# 4. CMake has no way of extending its property system to new kinds of objects.
#    So, for example, we have "this_set_property" instead of an extension
#    to "set_property".
#
# References
#   - LaTeX implementation of begin/end https://github.com/latex3/latex2e/blob/develop/base/ltmiscen.dtx

# -------------------------------------------------------
# Using environments
# -------------------------------------------------------
#
# This section describes the generic, shared syntax of all environments. It's
# minimal, since it's only about delimiting blocks of code. See the
# documentation for specific environments for how to use them.
#
# `commence( <environment-name> )`
# - [in] environment-name: The name of the environment
#
# `conclude( <environment-name> )`
# - [in] environment-name: The name of the environment
# - Precondition: the environment name must match that of the current open
#     environment.
#
# `commence` opens an environment block. The call to `commence` itself requires
# no other arguments, but particular environments may require additional
# arguments. It's conventional that an identifier for an environment be the
# first additional argument. For example the `define_environment` environment
# requires the name of the new environment as an additional argument.
#
# `conclude` closes an environment block. There must be an open environment, and
# its argument must the name of that environment.
#
# Environment blocks may be nested.
#
# Environments allow the created of context-specific data through environment
# properties. These are analogous to the CMake language properties but are
# implemented separately. The properties of an environment are only available
# within that environment. Properties may be used explicitly in user code as
# something like a block-local variable, but are more often used to implement
# environments.
#
# Environment names are identifiers for environments, properties, and for
# names that specific environments require. Valid environment names consist
# only of those characters valid in CMake variable names, namely, alphanumeric
# characters and the underscore. This is a technical limitation, because we
# incorporate environment names into the names of CMake variables.

# -------------------------------------------------------
# Defining new environments
# -------------------------------------------------------
#
# "define_environment" is an environment for defining new environments. We
# manually implement "define_environment" in order to define it without infinite
# recursion. All other environments are defined with "define_environment".
#
# commence( define_environment <environment-name> )
# - [in] <environment-name>
# - Precondition: <environment-name> must be a valid environment name
#
# Environments have three data:
# - Environment name. This is the argument to `define_environment`. This
#     parameter is mandatory.
# - Commence hook. This macro or function runs during `commence()`. If the
#     commence-hook fails, then the end hook does not run. This parameter is
#     optional. The parameter is set by assigning a macro or function name to
#     the property `commence_hook`.
# - Conclude hook. This macro or function runs during `conclude()`. This
#     parameter is optional. The parameter is set by assigning a macro or
#     function name to the property `conclude_hook`.
#
# `<commence_hook>( Success_Variable [Args...] )`
# - [out] Result_Variable: The begin-hook returns success by assigning "true" to
#     this variable. It return failure by assigning "false".
# - [in] Args...: All the arguments passed to `commence()`
#
# `<conclude_hook>( Success_Variable [Args...] )`
# - [in] Args...: All the arguments passed to `conclude()`
#
# Both the commence and conclude hooks run *in the same context* that the calls
# `commence()` and `conclude()` run in. They may be defined either as macros or
# functions insofar as the environment system is concerned. There are some
# consequences:
# - If defined as macros:
#   - Makes setting properties easy, because there's no need to manually forward
#     property values to the parent scope.
#   - Has hazard of name pollution with temporary variables. All variables
#     should begin with a unique prefix.
#   - May not call `return()`.
# - If defined as functions:
#   - Setting properties in a function body will not set them in the context
#     of the caller. This means that property values have to be explicitly set
#     in the parent scope.
#     - Note: At present there is no utility function to do this.
#   - Temporary variables may be used at will.
#   - `return()` is safe.
#
# Recommended practice for commence and conclude hooks:
# - Define the hook as a macro, but also define a tightly-bound function for it
#   that's an implementation detail of the macro.
# - Perform input validation etc. in the function. Variables that need to be
#   set in the macro can be set directly in the parent scope of the function.
# - Set properties in the macro. Do as much as possible otherwise in the
#   function.
#
# Recommended practice for defining environments
# - Put the environment definition into a separate `.cmake` module and
#   `include()` it when the environment is needed.
# - Use `include_guard()` at the top of the `.cmake` file. Environments don't
#   like being defined twice.

# -------------------------------------------------------
# Implementation of the services
# -------------------------------------------------------
#
# - Properties are forwarded to properties on a particular variable. The name of
#     the variable is thus:
#     1. A global prefix for environment names
#     2. The name of the environment
# - The prefix is the concatenation of property-variable name and an underscore.

#
# Debugging flag for the environment system
#
# If "false", this flag turns off invariant checks etc. for performance
#
set(TileDB_Environment_Debug ON)

#
# The block nesting level
#
# This variable is a cross-check against the length of the block list.
#
set(TileDB_Environment_Level 1)

#
# The block nesting list
#
# Invariant: TileDB_Environment_Level == length of TileDB_Environment_Blocks
#
# This list contains the names of all current blocks that have commenced but
# not yet concluded.
#
list(APPEND TileDB_Environment_Blocks "<root>")

#
# The current error state
#
# Invariant: TileDB_Environment_Level == length of TileDB_Environment_Errors
#
# We need one of these per environment, since a problem within a `commence`
# statement needs to be known during `conclude`.
#
list(APPEND TileDB_Environment_Errors false)

#
# The current `This`
#
# Invariant: TileDB_Environment_Level == length of TileDB_Environment_This
#
# We use a single name `This` in multiple environments, so we need to restore it
# whenever we leave an environment.
#
list(APPEND TileDB_Environment_This "<nothing>")

#
# Namespace initial strings for defined environments
#
# The concatenation of one of these strings and an environment forms a namespace
# for that environment. This is not the same as the variable prefix for the
# namespace, which is this string plus an underscore. We use multiple namespaces
# here to avoid any problems with malicious naming, such as (with a different
# naming convention, "foo" and "foo_begin", where "foo_begin" might be the name
# both of the begin-hook of environment "foo" and also an environment itself.
#
# Each of the prefixes is of the form `TileDB_Environment_<keyword>_`. The
# keywords:
# - Named: A variable of this name is created for each environment that stores
# properties for the environment type (but not for individual environments).
# - Begin: The name of the commence-hook for an environment
# - End: The name of the conclude-hook for an environment
# - This: The name of `This` variables
#

# -------------------------------------------------------
# Environment names
# -------------------------------------------------------

#
# Validate a name used in the environment system, be that an environment
# name, a property name, or any other name that's used to construct an internal
# variable identifier.
#
function(validate_environment_name Environment Valid)
    #
    # The regex will match either the whole string or nothing, so the result
    # will be either zero or one.
    #
    string(REGEX MATCHALL "^[0-9A-Za-z_]+$" Result ${Environment})
    list(LENGTH Result N)
    if (N EQUAL 1)
        set(Valid true PARENT_SCOPE)
    else()
        set(Valid false PARENT_SCOPE)
        message(SEND_ERROR "Invalid name \"${Environment}\" contains something other than alphanumeric or underscore.")
    endif()
endfunction()

# -------------------------------------------------------
# Stacks
# -------------------------------------------------------
#
# All the stack manipulation operations are implemented as macros. In short,
# this is a consequence of the brain-dead CMake scoping limitations. These
# operations are called by the `body` parts of `commence` and `conclude`, which
# are defined as functions (that requirement coming from a different CMake
# limitation). List operations only work on local-scope variables (yet another
# limitation), so to have push/pop on the global stack, the local values need
# to be propagated. Propagation, though, only goes up a single level. By
# defining these as macros and calling them *only* from the body functions,
# we can have operations on the top-level stack variables.
#

macro(push_block_good Name This)
    list(APPEND TileDB_Environment_Blocks "${Name}")
    set(TileDB_Environment_Blocks "${TileDB_Environment_Blocks}" PARENT_SCOPE)
    list(APPEND TileDB_Environment_Errors true)
    set(TileDB_Environment_Errors "${TileDB_Environment_Errors}" PARENT_SCOPE)
    list(APPEND TileDB_Environment_This "${This}")
    set(TileDB_Environment_This "${TileDB_Environment_This}" PARENT_SCOPE)
endmacro()

macro(push_block_bad)
    list(APPEND TileDB_Environment_Blocks "<error>")
    set(TileDB_Environment_Blocks "${TileDB_Environment_Blocks}" PARENT_SCOPE)
    list(APPEND TileDB_Environment_Errors false)
    set(TileDB_Environment_Errors "${TileDB_Environment_Errors}" PARENT_SCOPE)
    list(APPEND TileDB_Environment_This "<nothing>")
    set(TileDB_Environment_This "${TileDB_Environment_This}" PARENT_SCOPE)
endmacro()

#
# The scopes in this macro are not all the same:
# - parent: TileDB_Environment_Blocks, TileDB_Environment_Errors
# - local: Name_Var, Error_Var
#
macro(pop_block Name_Var Error_Var This_Var)
    list(POP_BACK TileDB_Environment_Blocks Value)
    set(TileDB_Environment_Blocks "${TileDB_Environment_Blocks}" PARENT_SCOPE)
    set(${Name_Var} "${Value}")

    list(POP_BACK TileDB_Environment_Errors Value)
    set(TileDB_Environment_Errors "${TileDB_Environment_Errors}" PARENT_SCOPE)
    set(${Error_Var} "${Value}")

    list(POP_BACK TileDB_Environment_This Value)
    set(TileDB_Environment_This "${TileDB_Environment_This}" PARENT_SCOPE)
    set(${This_Var} "${Value}")
endmacro()

function(is_stack_empty Result_Var)
    if (${TileDB_Environment_Level} EQUAL 1)
        set(${Result_Var} true PARENT_SCOPE)
    else()
        set(${Result_Var} false PARENT_SCOPE)
    endif()
endfunction()

#
# Ensure all the stack invariants are satisfied. Signal a fatal error if not.
#
# Note: This function does not modify data, and thus does not incur scoping
# limitations.
#
function(verify_stack_invariants)
    if (NOT TileDB_Environment_Debug)
        return()
    endif()
    #
    # Verify that the level is the same as the stack sizes
    #
    list(LENGTH TileDB_Environment_Blocks N)
    if (NOT N EQUAL TileDB_Environment_Level)
        message(FATAL_ERROR "Environment: !!! Block stack size ${N} does not equal level ${TileDB_Environment_Level}")
    endif()
    list(LENGTH TileDB_Environment_Errors N)
    if (NOT N EQUAL TileDB_Environment_Level)
        message(FATAL_ERROR "Environment: !!! Error stack size ${N} does not equal level ${TileDB_Environment_Level}")
    endif()
    list(LENGTH TileDB_Environment_This N)
    if (NOT N EQUAL TileDB_Environment_Level)
        message(FATAL_ERROR "Environment: !!! `This` stack size ${N} does not equal level ${TileDB_Environment_Level}")
    endif()
endfunction()

#
# Verify the conditions outside of an environment. The stack must be empty.
# `This` must not be defined.
#
# This function is primarily for manual testing outside of this module. It's
# used only once in this source file, at very end, to verify initialization of
# this module.
#
function(verify_outside_of_environment)
    if (NOT TileDB_Environment_Debug)
        return()
    endif()
    # Stack size is initialized to 1 (so that stack variables are not empty)
    if (NOT 1 EQUAL TileDB_Environment_Level)
        message(FATAL_ERROR "Environment: !!! Stack size ${TileDB_Environment_Level} is not 1 outside of commence/conclude")
    endif()
    if (DEFINED This)
        message(FATAL_ERROR "Environment: !!! `This` is defined outside of an environment")
    endif()
endfunction()

# -------------------------------------------------------
# The `This` prefix
# -------------------------------------------------------
#
# `This` is a prefix for constructing environment-scoped values. There's no such
# thing as an environment scope in CMake, at least natively, so we have to
# create it. We do this as follows:
# - during commence()
#   - create a unique name to assign to `This`
#   - push that name onto the "this" stack
#   - assign the name to `This`
#   - assign the empty string to `${This}` in parent scope (creates variable)
#   - call the begin-hook
# - during conclude()
#   - call the end-hook
#   - pop the "this"" stack
#   - reset `This` from the popped value
# Both the begin-hook and the end-hook may rely on an initialized `This` value,
# in particular to use property services.

#
# The environment-specific variable. It's not set either (1) outside an
# environment or (2) inside of an erroneous `commence` block
#
unset(This)

#
# The counter for generating `This` variables
#
# This counter allows generation of unique environment names.
#
set(TileDB_Environment_Counter 0)

#
# Create a new value for a `This` variable
#
# This macro runs at function scope, one level deep. In order to update the
# counter, it has access the parent scope.
#
macro(create_this)
    math(EXPR TileDB_Environment_Counter "${TileDB_Environment_Counter}+1")
    set(TileDB_Environment_Counter ${TileDB_Environment_Counter})
    set(This "TileDB_Environment_This_${TileDB_Environment_Counter}")
endmacro()

# -------------------------------------------------------
# commence() and conclude()
# -------------------------------------------------------
#
# commence() and conclude() are implemented as macros so that they may
# introduce context-dependent variables into scope of the block. CMake does not
# provide such a scoping mechanism, so we have to create one ourselves. Because
# we're using a macro(), there arises an issue with error handling. There are
# three kinds of possibilities: (1) we can have every error be a fatal error and
# immediately terminate, (2) we can have deeply nested `if` blocks in sequence,
# or (3) we can put most of the operations into functions and explicitly keep
# track of error state so that we can define all expected variables, even if
# they don't have useful values. We'll be doing mostly (3) with a bit of (2) and
# (1) as appropriate.
#
# A critical point about the use of `function` here is that anything that the
# environment sees has to be set in the parent scope. This leads not only to
# a lot of syntax cruft, but also a need to pay particular attention to how
# subroutines are defined. Some functions are specifically designated to be
# called from these functions *only*. They set variables in parent scope and
# correct operation requires that they be invoked exactly one function-scope in
# from the directory scope.

#
# The function part of `commence` that runs before the begin-hook runs
#
function(commence_body_A Success_Var Environment_Name)
    set(Log_Level VERBOSE)
    message(${Log_Level} ">>> commence_body_A begin >>>")
    message(${Log_Level} "commence_body: ARGC=${ARGC}")
    message(${Log_Level} "commence_body: Success_Var=${Success_Var}")
    message(${Log_Level} "commence_body: Environment_Name=${Environment_Name}")
    #
    # There must be at least one argument
    #
    if (NOT ${ARGC} GREATER_EQUAL 1)
        message(SEND_ERROR "'commence()' requires at least one argument")
        push_block_bad()
        set(${Success_Var} false PARENT_SCOPE)
        return()
    endif()
    #
    # The argument must be of the correct form.
    #
    validate_environment_name(${Environment_Name} Valid)
    if (NOT ${Valid})
        # error message generated in validation function
        push_block_bad()
        set(${Success_Var} false PARENT_SCOPE)
        return()
    endif()
    #
    # The first argument must be the name of a defined environment
    #
    set(Name "TileDB_Environment_Named_${Environment_Name}")
    message(${Log_Level} "Variable for this environment=\"${Name}\"")
    if (NOT DEFINED ${Name})
        message(SEND_ERROR "`commence()` argument \"${Environment_Name}\" is not a defined environment")
        set(${Success_Var} false PARENT_SCOPE)
        push_block_bad()
        return()
    endif()

    set(${Success_Var} true PARENT_SCOPE)
    message(${Log_Level} "<<< commence_body_A end <<<")
endfunction()

#
# The function part of `commence` that runs after the begin-hook runs
#
# This runs in a function so that we don't need two version of push_block_bad(),
# one for own scope and one for parent scope.
#
function(commence_body_B Success_Var Hook_Result Environment_Name)
    set(Log_Level VERBOSE)
    message(${Log_Level} ">>> commence_body_B begin >>>")
    #
    # The begin-hook was called beforehand by our caller.
    #
    if (NOT ${Hook_Result})
        message(${Log_Level} "begin-hook for environment \"${Environment_Name}\" was not successful")
        set(${Success_Var} false PARENT_SCOPE)
        push_block_bad()
        return()
    endif()
    #
    # Push the name environment name onto the name stack and "no error" onto the
    # error stack
    #
    set(${Success_Var} true PARENT_SCOPE)
    push_block_good(${Environment_Name} ${This})
    message(${Log_Level} "<<< commence_body_B end <<<")
endfunction()

#
# Begin an environment block
#
# Usage: commence(<environment-name>)
# Precondition for success: <environment-name> must have been previously defined
# Postcondition on success: `This` is the name of a variable unique to this commence() statement
# Postcondition on fail: `This` is not defined
#
# The implementation of this function is in four sections:
# - A preliminary section in this scope
# - commence_body_A, which runs in function scope for error handling
# - A section that runs the begin-hook in this scope
# - commence_body_B, which runs in functions scope, again for error handling
#
macro(commence Environment_Name)
    set(TileDB_Environment_Log_Level_commence VERBOSE)
    message(${TileDB_Environment_Log_Level_commence} ">>> commence begin >>>")
    #
    # Increment the level counter before anything else.
    #
    if (TileDB_Environment_Level LESS 1)
        message(FATAL_ERROR "Environment commence: !!! Level went below one.")
    endif()
    math(EXPR TileDB_Environment_Level "${TileDB_Environment_Level}+1")

    commence_body_A(Success ${ARGV})
    if (${Success})
        #
        # Define `This` and `${This}`
        #
        create_this()
        set(${This} "")
        #
        # Call the begin-hook for the environment
        #
        set(Result true)
        message(${TileDB_Environment_Log_Level_commence} ">>> call begin-hook >>>")
        cmake_language(CALL "${TileDB_Environment_Begin_${Environment_Name}}" Result ${ARGN})
        message(${TileDB_Environment_Log_Level_commence} "<<< return begin-hook <<<")

        commence_body_B(Success Result ${ARGV})
    endif()
    # If either body_A or body_B had no success, then ${Success} is false
    if (NOT ${Success})
        # Bad environment, no `This` for you
        unset(${This})
        unset(This)
    endif()

    # if we have no problems setting up this environment
    #   - set up environment variables
    #
    verify_stack_invariants()
    message(${TileDB_Environment_Log_Level_commence} "<<< commence end <<<")
endmacro()

#
# The function body for `conclude()`.
#
function(conclude_body Success_Var Environment_Name)
    set(Log_Level VERBOSE)
    message(${Log_Level} ">>> conclude_body begin >>>")
    #
    # We always pop the stacks, since that's required to preserve the stack
    # invariants.
    #
    pop_block(Environment Good This_New)
    set(TileDB_Environment_conclude_This_New ${This_New} PARENT_SCOPE)
    if (NOT ${Good})
        # We reported errors during commence(), so don't do anything else now.
        set(${Success_Var} false PARENT_SCOPE)
        return()
    endif()
    #
    # Verify a matching pair with commence()
    #
    if (NOT ${Environment_Name} STREQUAL ${Environment})
        message(SEND_ERROR "conclude() argument \"${Environment_Name}\" does not match current environment \"${Environment}\".")
        set(${Success_Var} false PARENT_SCOPE)
        return()
    endif()

    set(${Success_Var} true PARENT_SCOPE)
    message(${Log_Level} "<<< conclude_body end <<<")
endfunction()

#
# End an environment block
#
# Usage: conclude(<environment-name>)
# Precondition:
#   - There must be a previously-commenced environment block that has not yet
#     concluded.
#   - The <environment-name> argument must have been the argument of the most
#     recent, unconcluded `commence` statement.
#
macro(conclude Environment_Name)
    set(TileDB_Environment_Log_Level_conclude VERBOSE)
    message(${TileDB_Environment_Log_Level_conclude} ">>> conclude begin >>>")
    #
    # Verify that we're in an open environment (commenced-not-concluded) and
    # decrement the level counter before anything else.
    #
    if (TileDB_Environment_Level LESS_EQUAL 1)
        message(FATAL_ERROR "Environment conclude: !!! Level may not go below one.")
    endif()
    math(EXPR TileDB_Environment_Level "${TileDB_Environment_Level}-1")

    conclude_body(Success ${ARGV})
    if(${Success})
        #
        # Call the conclude-hook for the environment
        #
        message(${TileDB_Environment_Log_Level_conclude} ">>> call end-hook >>>")
        cmake_language(CALL "${TileDB_Environment_End_${Environment_Name}}" ${ARGN})
        message(${TileDB_Environment_Log_Level_conclude} "<<< return end-hook <<<")
    endif()

    #
    # Restore `This`. As a special case, if we are restoring for an outermost
    # `conclude()` we must unset `This`.
    #
    is_stack_empty(TileDB_Environment_conclude_Empty)
    if (${TileDB_Environment_conclude_Empty})
        unset(This)
    else()
        set(This ${TileDB_Environment_conclude_This_New})
    endif()

    verify_stack_invariants()
    message(${TileDB_Environment_Log_Level_conclude} "<<< conclude end <<<")
endmacro()

# -------------------------------------------------------
# Environment Properties
# -------------------------------------------------------
#
# Properties are, underneath it all, variables whose names are constructed
# rather than constant. The names of environment properties are built up thus:
# - The `This` variable, which is a constant prefix and a unique counter value
# - The keyword "Property", to distinguish user-defined properties
# - The property name
#
# Can't use this syntax, because there's no variable scope
#     set_property(VARIABLE ${This} PROPERTY name "")

#
# Set a property value.
# - If there is no argument, the property is set to the empty string
# - If there is one argument, the property is set to that
# - If there is more than one argument, the property is set to a list
#   constructed from the arguments
#
function(set_this_property Property_Name)
    set(X ${ARGN})
    set(${This}_Property_${Property_Name} "${X}" PARENT_SCOPE)
endfunction()

function(unset_this_property Property_Name)
    unset(${This}_Property_${Property_Name} PARENT_SCOPE)
endfunction()

function(get_this_property Property_Name Value_Var)
    set(Property_Var ${This}_Property_${Property_Name})
    if (DEFINED ${Property_Var})
        set(${Value_Var} ${${Property_Var}} PARENT_SCOPE)
    else()
        unset(${Value_Var} PARENT_SCOPE)
    endif()
endfunction()

function(append_this_property Property_Name)
    set(Property_Var ${This}_Property_${Property_Name})
    # list() creates a new variable in this scope
    list(APPEND ${Property_Var} ${ARGN})
    set(${Property_Var} ${${Property_Var}} PARENT_SCOPE)
endfunction()

# -------------------------------------------------------
# The define_environment environment
# -------------------------------------------------------

# The begin-hook and the end-hook both have an initial argument for a succeed-
# or-fail argument. In order to actually return a value through this argument,
# it has to be set in the parent scope. For example:
# ```
# set(Result false PARENT_SCOPE)
# ```
# The result does not have to be set for successful return.

set("TileDB_Environment_Named_define_environment" "define_environment")
set("TileDB_Environment_Begin_define_environment" "define_environment_begin")
set("TileDB_Environment_End_define_environment" "define_environment_end")

#
# Macro used to substitute for hooks not explicitly specified.
#
macro(TileDB_Environment_null_macro)
endmacro()

#
#
#
macro(define_environment_begin Result Environment)
    set(Log_Level_define_environment_begin VERBOSE)
    message(${Log_Level_define_environment_begin} ">>> define_environment_begin begin >>>")
    message(${Log_Level_define_environment_begin} "  Environment=${Environment}")
    #
    # The argument must be of the correct form.
    #
    validate_environment_name(${Environment} Valid)
    if (NOT ${Valid})
        set(Result false)
        return()
    endif()
    # TODO: ensure the environment has not already been defined

    message(${Log_Level_define_environment_begin} "  This=${This}")
    #
    # Set the "name" property
    #
    set_this_property(name ${Environment})

    message(${Log_Level_define_environment_begin} "<<< define_environment_begin end <<<")
endmacro()

macro(define_environment_end)
    set(Log_Level_define_environment_end VERBOSE)
    message(${Log_Level_define_environment_end} ">>> define_environment_end begin >>>")

    get_this_property(name Name)
    message(${Log_Level_define_environment_end} "  environment name=${Name}")
    set("TileDB_Environment_Named_${Name}" "${Name}")

    get_this_property(commence_hook X)
    if (DEFINED X)
        set("TileDB_Environment_Begin_${Name}" "${X}")
    else()
        set("TileDB_Environment_Begin_${Name}" TileDB_Environment_null_macro)
    endif()

    get_this_property(conclude_hook X)
    if (DEFINED X)
        set("TileDB_Environment_End_${Name}" "${X}")
    else()
        set("TileDB_Environment_End_${Name}" TileDB_Environment_null_macro)
    endif()

    message(${Log_Level_define_environment_end} "<<< define_environment_end end <<<")
endmacro()

verify_outside_of_environment()
message(${TileDB_Environment_Log_Level} "<<< environment.cmake end <<<")

