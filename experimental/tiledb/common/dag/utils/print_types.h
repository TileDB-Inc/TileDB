/**
 * @file print_types.hpp
 *
 * @copyright SPDX-FileCopyrightText: 2022 Battelle Memorial Institute
 * @copyright SPDX-FileCopyrightText: 2022 University of Washington
 *
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * @authors
 *   Luke D'Alessandro
 *
 */

#ifndef PRINT_TYPES_HPP
#define PRINT_TYPES_HPP

template <class... Ts>
struct print_types_t;

template <class... Ts>
constexpr auto print_types(Ts...) {
  return print_types_t<Ts...>{};
}

#endif // PRINT_TYPES_HPP
