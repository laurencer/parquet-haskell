{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}

module Data.Parquet.Internal.Encodings(
    readRLE
  , readUnsignedVarInt
  , widthFromMaxInt
  , readBitPacked
  , readRLEBitPackingHybrid
) where

import Prelude hiding (take)

import Control.Applicative
import Control.Monad

import           Data.Attoparsec.ByteString
import           Data.Attoparsec.Binary
import           Data.DoubleWord
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BSC
import qualified Data.HashMap.Strict as Map
import           Data.Int
import           Data.Bits
import           Data.Binary.IEEE754
import           Data.Word
import qualified Data.BitVector as BV

import           Data.Parquet.Internal.FileMetadataTypes
import           Data.Parquet.Internal.Enums
import           Data.Parquet.Internal.Thrift.Types
import           Data.Parquet.Internal.Thrift.Parsers

import Debug.Trace


type BitWidth = Int
-- | Reads a run-length encoded run.

-- RLE encodings

-- | First byte of the Hybrid encoding (used to determine whether it is RLE or bitpacked)
type Header = Int

fixed :: Int -> Parser a -> Parser a
fixed i p = do
    intermediate <- take i
    case parseOnly (p <* endOfInput) intermediate of
        Left _ -> empty
        Right x -> return x

readRLEBitPackingHybrid :: BitWidth -> Parser [BS.ByteString]
readRLEBitPackingHybrid bitWidth = do
  length  <- fromIntegral <$> anyWord32le
  fixed length parser
  where parser = many' parseSingle
        parseSingle = do
          header  <- readUnsignedVarInt
          if (header .&. 1) == 0 then (readRLE bitWidth header) else (readBitPacked bitWidth header)

readRLE :: Header -> BitWidth -> Parser BS.ByteString
readRLE header bitWidth = do
  count   <- return $ header `shiftR` 1
  width   <- return $ byteWidth bitWidth
  bytes   <- take width
  return $ BS.append bytes (BS.replicate (4 - (BS.length bytes)) zeroBits)

-- | Parser for a Hybrid BitPacked Header
--   >>> parseOnly (readBitPacked 9 5) (BSC.pack "\x20\x88\x41\x8a\x39\x28\xa9\xc5\x9a\x7b\x30\xca\x49\xab\xbd\x18")
--   Done "" [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24]
--
readBitPacked :: Header -> BitWidth -> Parser BS.ByteString
readBitPacked header width = do
  rawBytes  <- takeByteString -- byteCount
  return $ BSC.pack $ show $ process (bits rawBytes)
  where groupsCount = (header `shiftR` 1)
        byteCount :: Int
        byteCount = width * groupsCount
        bits :: BS.ByteString -> BV.BitVector
        bits bs = BV.fromBits $ BS.foldr' (\el acc -> (BV.toBits (BV.bitVec 8 el)) ++ acc) [] bs 
        -- total number of bits read
        bitCount = byteCount * 8
        -- positions where new data starts
        bitDividers :: Int -> [Int]
        bitDividers totalBits = [0,width..totalBits]
        -- ranges where we have bits.
        process :: BV.BitVector -> [Integer]
        process bs = map extract bitRanges
          where extract :: (Int, Int) -> Integer
                extract (start, end) = BV.nat (bs BV.@@ (size' - start, size' - end + 1))
                size' = BV.size bs
                dividers = bitDividers size'
                bitRanges = zip dividers ((\x -> x - 1) <$> tail dividers)


-- Helpers

widthFromMaxInt :: Int -> Int
widthFromMaxInt value = 
  fromIntegral (ceiling (log (doubleValue + 1)))
  where doubleValue :: Double
        doubleValue = fromIntegral value

byteWidth :: Int -> Int
byteWidth bitWidth = floor $ (doubleValue + 7) / 8
  where doubleValue :: Double
        doubleValue = fromIntegral bitWidth

-- Generic encodings

-- | Decodes a variable length integer
--   >>> parse readUnsignedVarInt (BSC.pack "\x1F")
--   Done "" 31
--   >>> parse readUnsignedVarInt (BSC.pack "\xFF\x00")
--   Done "" 127
--   >>> parse readUnsignedVarInt (BSC.pack "\xFF\xFF\xFF\xFF\xFF\xFF\10")
--   Done "" 48378511622143
readUnsignedVarInt :: Parser Int
readUnsignedVarInt = do
  word32 <- iterate 0 0
  return $ fromIntegral word32
    where
      i64 :: Integral a => a -> Int64
      i64 a = fromIntegral a
      iterate result shiftAmount = (continue result shiftAmount) <|> (stop result)
      stop result = (return result) <* endOfInput
      continue :: Int64 -> Int -> Parser Int64
      continue result shiftAmount = do
        byte    <- i64 <$> anyWord8
        current <- return $ result .|. ((byte .&. 0x7F) `shiftL` shiftAmount)
        if ((byte .&. 0x80) == 0) then
          return current
          else iterate current (shiftAmount + 7)

-- Plain encodings

readPlainInt32 :: Parser Int32
readPlainInt32 = fromIntegral <$> anyWord32le

readPlainInt64 :: Parser Int64
readPlainInt64 = fromIntegral <$> anyWord64le

readPlainInt96 :: Parser Int96
readPlainInt96 = do
  first  <- anyWord64le
  second <- anyWord32le
  return $ Int96 (fromIntegral second) first

readPlainFloat :: Parser Float
readPlainFloat = wordToFloat <$> anyWord32le

readPlainDouble :: Parser Double
readPlainDouble = wordToDouble <$> anyWord64le

readPlainByteArray :: Parser BS.ByteString
readPlainByteArray = do
  length <- readPlainInt32
  take (fromIntegral length)

readPlainByteArrayFixed :: Int -> Parser BS.ByteString
readPlainByteArrayFixed num = take num

