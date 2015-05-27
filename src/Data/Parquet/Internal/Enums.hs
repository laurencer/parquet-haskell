{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Data.Parquet.Internal.Enums(
    ParquetType
  , ConvertedType
  , PageType
  , CompressionCodec
  , Encoding
  , FieldRepetitionType(..)
) where

import Control.Applicative

import           Data.Int

import           Data.Parquet.Internal.Thrift.Types
import           Data.Parquet.Internal.Thrift.Parsers

data ParquetType = BOOLEAN
                 | INT32
                 | INT64
                 | INT96
                 | FLOAT
                 | DOUBLE
                 | BYTE_ARRAY
                 | FIXED_LEN_BYTE_ARRAY
                 | UNKNOWN_PARQUET_TYPE Int
                 deriving (Show, Eq, Ord)

instance Enum ParquetType where
  fromEnum t = case t of
    BOOLEAN -> 0
    INT32 -> 1
    INT64 -> 2
    INT96 -> 3
    FLOAT -> 4
    DOUBLE -> 5
    BYTE_ARRAY -> 6
    FIXED_LEN_BYTE_ARRAY -> 7

  toEnum t = case t of
    0 -> BOOLEAN
    1 -> INT32
    2 -> INT64
    3 -> INT96
    4 -> FLOAT
    5 -> DOUBLE
    6 -> BYTE_ARRAY
    7 -> FIXED_LEN_BYTE_ARRAY
    t -> UNKNOWN_PARQUET_TYPE t

data ConvertedType = UTF8
                   | MAP
                   | MAP_KEY_VALUE
                   | LIST
                   | ENUM
                   | DECIMAL
                   | DATE
                   | TIME_MILLIS
                   | TIMESTAMP_MILLIS
                   | UINT_8
                   | UINT_16
                   | UINT_32
                   | UINT_64
                   | INT_8
                   | INT_16
                   | INT_32
                   | INT_64
                   | JSON
                   | BSON
                   | INTERVAL 
                   | UNKNOWN_CONVERTED_TYPE Int
                   deriving (Show, Eq, Ord)

instance Enum ConvertedType where
  fromEnum t = case t of
    UTF8 -> 0
    MAP -> 1
    MAP_KEY_VALUE -> 2
    LIST -> 3
    ENUM -> 4
    DECIMAL -> 5
    DATE -> 6
    TIME_MILLIS -> 7
    TIMESTAMP_MILLIS -> 9
    UINT_8 -> 11
    UINT_16 -> 12
    UINT_32 -> 13
    UINT_64 -> 14
    INT_8 -> 15
    INT_16 -> 16
    INT_32 -> 17
    INT_64 -> 18
    JSON -> 19
    BSON -> 20
    INTERVAL -> 21
  toEnum t = case t of
    0 -> UTF8
    1 -> MAP
    2 -> MAP_KEY_VALUE
    3 -> LIST
    4 -> ENUM
    5 -> DECIMAL
    6 -> DATE
    7 -> TIME_MILLIS
    9 -> TIMESTAMP_MILLIS
    11 -> UINT_8
    12 -> UINT_16
    13 -> UINT_32
    14 -> UINT_64
    15 -> INT_8
    16 -> INT_16
    17 -> INT_32
    18 -> INT_64
    19 -> JSON
    20 -> BSON
    21 -> INTERVAL
    t  -> UNKNOWN_CONVERTED_TYPE t

data FieldRepetitionType = REQUIRED
                         | OPTIONAL
                         | REPEATED
                         deriving (Show, Eq, Ord)

instance Enum FieldRepetitionType where
  fromEnum t = case t of
    REQUIRED -> 0
    OPTIONAL -> 1
    REPEATED -> 2
  toEnum t = case t of
    0 -> REQUIRED
    1 -> OPTIONAL
    2 -> REPEATED

data Encoding = PLAIN
              | PLAIN_DICTIONARY
              | RLE
              | BIT_PACKED
              | DELTA_BINARY_PACKED
              | DELTA_LENGTH_BYTE_ARRAY
              | DELTA_BYTE_ARRAY
              | RLE_DICTIONARY 
              | UNKNOWN_ENCODING Int
              deriving (Show, Eq, Ord)

instance Enum Encoding where
  fromEnum t = case t of
    PLAIN -> 0
    PLAIN_DICTIONARY -> 2
    RLE -> 3
    BIT_PACKED -> 4
    DELTA_BINARY_PACKED -> 5
    DELTA_LENGTH_BYTE_ARRAY -> 6
    DELTA_BYTE_ARRAY -> 7
    RLE_DICTIONARY -> 8
  toEnum t = case t of
    0 -> PLAIN
    2 -> PLAIN_DICTIONARY
    3 -> RLE
    4 -> BIT_PACKED
    5 -> DELTA_BINARY_PACKED
    6 -> DELTA_LENGTH_BYTE_ARRAY
    7 -> DELTA_BYTE_ARRAY
    8 -> RLE_DICTIONARY
    t -> UNKNOWN_ENCODING t

data CompressionCodec = UNCOMPRESSED
                      | SNAPPY
                      | GZIP
                      | LZO 
                      | UNKNOWN_COMPRESSION Int
                      deriving (Show, Eq, Ord)

instance Enum CompressionCodec where
  fromEnum t = case t of
    UNCOMPRESSED -> 0
    SNAPPY -> 1
    GZIP -> 2
    LZO -> 3
  toEnum t = case t of
    0 -> UNCOMPRESSED
    1 -> SNAPPY
    2 -> GZIP
    3 -> LZO
    t -> UNKNOWN_COMPRESSION t

data PageType = DATA_PAGE
              | INDEX_PAGE
              | DICTIONARY_PAGE
              | DATA_PAGE_V2 
              | UNKNOWN_PAGE_TYPE Int
              deriving (Show, Eq, Ord)

instance Enum PageType where
  fromEnum t = case t of
    DATA_PAGE -> 0
    INDEX_PAGE -> 1
    DICTIONARY_PAGE -> 2
    DATA_PAGE_V2 -> 3
  toEnum t = case t of
    0 -> DATA_PAGE
    1 -> INDEX_PAGE
    2 -> DICTIONARY_PAGE
    3 -> DATA_PAGE_V2
    t -> UNKNOWN_PAGE_TYPE t
