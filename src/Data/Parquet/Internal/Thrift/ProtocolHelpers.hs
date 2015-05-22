{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE OverloadedStrings #-}
--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements. See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership. The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License. You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied. See the License for the
-- specific language governing permissions and limitations
-- under the License.
--

module Data.Parquet.Internal.Thrift.ProtocolHelpers
    ( 
      getTypeOf
    , version1
    , bsToDouble
    ) where

import Control.Exception
import Data.Attoparsec.ByteString
import Data.Bits
import Data.ByteString.Lazy (ByteString, toStrict)
import Data.ByteString.Unsafe
import Data.Functor ((<$>))
import Data.Int
import Data.Monoid (mempty)
import Data.Text.Lazy (Text)
import Data.Typeable (Typeable)
import Data.Word
import Foreign.Ptr (castPtr)
import Foreign.Storable (Storable, peek, poke)
import System.IO.Unsafe
import qualified Data.ByteString as BS
import qualified Data.HashMap.Strict as Map

import Data.Parquet.Internal.Thrift.Types

version1 :: Int32
version1 = fromIntegral (0x80010000 :: Word32)

getTypeOf :: ThriftVal -> ThriftType
getTypeOf v =  case v of
  TStruct{} -> T_STRUCT Map.empty
  TMap{} -> T_MAP T_VOID T_VOID
  TList{} -> T_LIST T_VOID
  TSet{} -> T_SET T_VOID
  TBool{} -> T_BOOL
  TByte{} -> T_BYTE
  TI16{} -> T_I16
  TI32{} -> T_I32
  TI64{} -> T_I64
  TString{} -> T_STRING
  TDouble{} -> T_DOUBLE

handleEOF :: SomeException -> IO BS.ByteString
handleEOF = const $ return mempty

-- | Converts a ByteString to a Floating point number
-- The ByteString is assumed to be encoded in network order (Big Endian)
-- therefore the behavior of this function varies based on whether the local
-- machine is big endian or little endian.
bsToDouble :: BS.ByteString -> Double
bsToDouble bs = unsafeDupablePerformIO $ unsafeUseAsCString bs castBs
  where
#if __BYTE_ORDER == __LITTLE_ENDIAN
    castBs chrPtr = do
      w <- peek (castPtr chrPtr)
      poke (castPtr chrPtr) (byteSwap w)
      peek (castPtr chrPtr)
#else
    castBs = peek . castPtr
#endif

#if __BYTE_ORDER == __LITTLE_ENDIAN
-- | Swap endianness of a 64-bit word
byteSwap :: Word64 -> Word64
byteSwap w = (w `shiftL` 56 .&. 0xFF00000000000000) .|.
             (w `shiftL` 40 .&. 0x00FF000000000000) .|.
             (w `shiftL` 24 .&. 0x0000FF0000000000) .|.
             (w `shiftL` 8  .&. 0x000000FF00000000) .|.
             (w `shiftR` 8  .&. 0x00000000FF000000) .|.
             (w `shiftR` 24 .&. 0x0000000000FF0000) .|.
             (w `shiftR` 40 .&. 0x000000000000FF00) .|.
             (w `shiftR` 56 .&. 0x00000000000000FF)
#endif
