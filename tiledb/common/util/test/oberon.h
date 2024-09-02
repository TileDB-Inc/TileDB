/**
 * @file   oberon.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * Oberon's monologue from a Midsummer Night's Dream, Public Domain
 * The verses of iambic pentameter are divided into 10 syllables, which don't
 * seem quite like 10 syllables, but I'm a computer scientist, not a poet.
 *
 *     I know a bank where the wild thyme blows,
 *     Where oxlips and the nodding violet grows,
 *     Quite over-canopied with luscious woodbine,
 *     With sweet musk-roses, and with eglantine:
 *
 * This file provides different representations of the text, for testing.
 * Included are
 *   - contiguous strings, along with the partition vector delimiting the
 *     positions of the syllable breaks
 *   - vectors of strings, along with their expected self-sorted order
 *   - vectors of vectors of characters, along with their expected order
 *
 * In addition to "self-sorted" order for each verse, we also provide expected
 * results for verses 2, 3, and 4 when sorted by the order by which verse 1 (the
 * first verse) is sorted.  Note that in those cases, verses 2, 3, and 4 will
 * not be in self-sorted order.
 *
 * Note that although the syllable breaks are not at the same position for each
 * of the verses, each verse has the same number of syllables.
 */

#ifndef TILEDB_OBERON_H
#define TILEDB_OBERON_H

#include <string>
#include <vector>

namespace oberon {
/*
 * As contiguous strings, along with the partition vector delimiting the
 * positions of the syllable breaks.
 */
std::string cs0 = "Iknowabankwherethewildthymeblows";
std::vector<size_t> ps0 = {0, 1, 5, 6, 10, 15, 18, 20, 22, 27, 32};

std::string cs1 = "Whereoxlipsandthenoddingvioletgrows";
std::vector<size_t> ps1 = {0, 5, 7, 11, 14, 17, 20, 24, 27, 30, 35};

std::string cs2 = "Quiteovercanopiedwithlusciouswoodbine";
std::vector<size_t> ps2 = {0, 5, 9, 12, 13, 17, 21, 24, 29, 33, 37};

std::string cs3 = "Withsweetmuskrosesandwitheglantine";
std::vector<size_t> ps3 = {0, 4, 9, 13, 15, 18, 21, 25, 27, 30, 34};

/*
 * As vectors of strings ("vs"), along with their expected self-sorted order,
 * and their order as sorted by vs0.
 */
std::vector<std::string> vs0{
    "I",
    "know",
    "a",
    "bank",
    "where",
    "the",
    "wi",
    "ld",
    "thyme",
    "blows",
};

std::vector<std::string> vs0_expected{
    "I",
    "a",
    "bank",
    "blows",
    "know",
    "ld",
    "the",
    "thyme",
    "where",
    "wi",
};

std::vector<std::string> vs0_by_vs0_expected{
    "I",
    "a",
    "bank",
    "blows",
    "know",
    "ld",
    "the",
    "thyme",
    "where",
    "wi",
};

std::vector<std::string> vs1{
    "Where",
    "ox",
    "lips",
    "and",
    "the",
    "nod",
    "ding",
    "vio",
    "let",
    "grows",
};

std::vector<std::string> vs1_expected{
    "Where",
    "and",
    "ding",
    "grows",
    "let",
    "lips",
    "nod",
    "ox",
    "the",
    "vio",
};

std::vector<std::string> vs1_by_vs0_expected{
    "Where",
    "lips",
    "and",
    "grows",
    "ox",
    "vio",
    "nod",
    "let",
    "the",
    "ding",
};

std::vector<std::string> vs2{
    "Quite",
    "over",
    "can",
    "o",
    "pied",
    "with",
    "lus",
    "cious",
    "wood",
    "bine",
};

std::vector<std::string> vs2_expected{
    "Quite",
    "bine",
    "can",
    "cious",
    "lus",
    "o",
    "over",
    "pied",
    "with",
    "wood",
};

std::vector<std::string> vs2_by_vs0_expected{
    "Quite",
    "can",
    "o",
    "bine",
    "over",
    "cious",
    "with",
    "pied",
    "wood",
    "lus",
};

std::vector<std::string> vs3{
    "With",
    "sweet",
    "musk",
    "ro",
    "ses",
    "and",
    "with",
    "eg",
    "lan",
    "tine",
};

std::vector<std::string> vs3_expected{
    "With",
    "and",
    "eg",
    "lan",
    "musk",
    "ro",
    "ses",
    "sweet",
    "tine",
    "with",
};

std::vector<std::string> vs3_by_vs0_expected{
    "With",
    "musk",
    "ro",
    "tine",
    "sweet",
    "eg",
    "and",
    "lan",
    "ses",
    "with",
};

/*
 * As vectors of vectors of characters ("vvc"), along with their expected
 * self-sorted order.
 */
std::vector<std::vector<char>> vvc0{
    {'I'},
    {'k', 'n', 'o', 'w'},
    {'a'},
    {'b', 'a', 'n', 'k'},
    {'w', 'h', 'e', 'r', 'e'},
    {'t', 'h', 'e'},
    {'w', 'i'},
    {'l', 'd'},
    {'t', 'h', 'y', 'm', 'e'},
    {'b', 'l', 'o', 'w', 's'},
};

std::vector<std::vector<char>> vvc0_expected{
    {'I'},
    {'a'},
    {'b', 'a', 'n', 'k'},
    {'b', 'l', 'o', 'w', 's'},
    {'k', 'n', 'o', 'w'},
    {'l', 'd'},
    {'t', 'h', 'e'},
    {'t', 'h', 'y', 'm', 'e'},
    {'w', 'h', 'e', 'r', 'e'},
    {'w', 'i'},
};

std::vector<std::vector<char>> vvc0_by_vvc0_expected{
    {'I'},
    {'a'},
    {'b', 'a', 'n', 'k'},
    {'b', 'l', 'o', 'w', 's'},
    {'k', 'n', 'o', 'w'},
    {'l', 'd'},
    {'t', 'h', 'e'},
    {'t', 'h', 'y', 'm', 'e'},
    {'w', 'h', 'e', 'r', 'e'},
    {'w', 'i'},
};

std::vector<std::vector<char>> vvc1{
    {'W', 'h', 'e', 'r', 'e'},
    {'o', 'x'},
    {'l', 'i', 'p', 's'},
    {'a', 'n', 'd'},
    {'t', 'h', 'e'},
    {'n', 'o', 'd'},
    {'d', 'i', 'n', 'g'},
    {'v', 'i', 'o'},
    {'l', 'e', 't'},
    {'g', 'r', 'o', 'w', 's'},
};

std::vector<std::vector<char>> vvc1_expected{
    {'W', 'h', 'e', 'r', 'e'},
    {'a', 'n', 'd'},
    {'d', 'i', 'n', 'g'},
    {'g', 'r', 'o', 'w', 's'},
    {'l', 'e', 't'},
    {'l', 'i', 'p', 's'},
    {'n', 'o', 'd'},
    {'o', 'x'},
    {'t', 'h', 'e'},
    {'v', 'i', 'o'},
};

std::vector<std::string> vvc1_by_vvc0_expected{
    {'W', 'h', 'e', 'r', 'e'},
    {'l', 'i', 'p', 's'},
    {'a', 'n', 'd'},
    {'g', 'r', 'o', 'w', 's'},
    {'o', 'x'},
    {'v', 'i', 'o'},
    {'n', 'o', 'd'},
    {'l', 'e', 't'},
    {'t', 'h', 'e'},
    {'d', 'i', 'n', 'g'},
};

std::vector<std::vector<char>> vvc2{
    {'Q', 'u', 'i', 't', 'e'},
    {'o', 'v', 'e', 'r'},
    {'c', 'a', 'n'},
    {'o'},
    {'p', 'i', 'e', 'd'},
    {'w', 'i', 't', 'h'},
    {'l', 'u', 's'},
    {'c', 'i', 'o', 'u', 's'},
    {'w', 'o', 'o', 'd'},
    {'b', 'i', 'n', 'e'},
};
std::vector<std::vector<char>> vvc2_expected{
    {'Q', 'u', 'i', 't', 'e'},
    {'b', 'i', 'n', 'e'},
    {'c', 'a', 'n'},
    {'c', 'i', 'o', 'u', 's'},
    {'l', 'u', 's'},
    {'o'},
    {'o', 'v', 'e', 'r'},
    {'p', 'i', 'e', 'd'},
    {'w', 'i', 't', 'h'},
    {'w', 'o', 'o', 'd'},
};
std::vector<std::vector<char>> vvc3{
    {'W', 'i', 't', 'h'},
    {'s', 'w', 'e', 'e', 't'},
    {'m', 'u', 's', 'k'},
    {'r', 'o'},
    {'s', 'e', 's'},
    {'a', 'n', 'd'},
    {'w', 'i', 't', 'h'},
    {'e', 'g'},
    {'l', 'a', 'n'},
    {'t', 'i', 'n', 'e'},
};
std::vector<std::vector<char>> vvc3_expected{
    {'W', 'i', 't', 'h'},
    {'a', 'n', 'd'},
    {'e', 'g'},
    {'l', 'a', 'n'},
    {'m', 'u', 's', 'k'},
    {'r', 'o'},
    {'s', 'e', 's'},
    {'s', 'w', 'e', 'e', 't'},
    {'t', 'i', 'n', 'e'},
    {'w', 'i', 't', 'h'},
};

std::vector<size_t> perm0 = {0, 2, 3, 9, 1, 7, 5, 8, 4, 6};
std::vector<size_t> perm1 = {0, 3, 6, 9, 8, 2, 5, 1, 4, 7};
std::vector<size_t> perm2 = {0, 9, 2, 7, 6, 3, 1, 4, 5, 8};
std::vector<size_t> perm3 = {0, 5, 7, 8, 2, 3, 4, 1, 9, 6};

}  // namespace oberon
#endif  //
