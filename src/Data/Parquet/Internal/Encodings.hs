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
import           Data.Binary.Get
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
type EncodedInt = Int96

-- | Runs the subparser with a fixed amount of data.
fixed :: Int -> Parser a -> Parser a
fixed bytesToParse subparser = do
    intermediate <- take bytesToParse
    case parseOnly (subparser <* endOfInput) intermediate of
        Left _ -> empty
        Right x -> return x

readRLEBitPackingHybrid :: BitWidth -> Parser [[EncodedInt]]
readRLEBitPackingHybrid bitWidth = do
  length  <- fromIntegral <$> anyWord32le
  fixed length parser
  where parser = many' parseSingle
        parseSingle = do
          header  <- readUnsignedVarInt
          if (header .&. 1) == 0 then (readRLE bitWidth header) else (readBitPacked bitWidth header)

-- | Parser for an Hybrid RLE value
--   >>> parseOnly (readRLE 50 1) (BSC.pack "\x01")
--   Right [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
--
readRLE :: Header -> BitWidth -> Parser [EncodedInt]
readRLE header bitWidth = do
  count   <- return $ header `shiftR` 1
  width   <- return $ byteWidth bitWidth
  bytes   <- take width
  padded  <- return $ BS.append bytes (BS.replicate (4 - (BS.length bytes)) zeroBits)
  value   <- return $ runGet (getWord32le) (LBS.fromStrict padded)
  return $ replicate count (fromIntegral value)


-- | Parser for a Hybrid BitPacked value
--   >>> parseOnly (readBitPacked 9 5) (BSC.pack "\x20\x88\x41\x8a\x39\x28\xa9\xc5\x9a\x7b\x30\xca\x49\xab\xbd\x18")
--   Right [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24]
--
readBitPacked :: Header -> BitWidth -> Parser [EncodedInt]
readBitPacked header width = do
  rawBytes  <- takeByteString -- take byteCount
  return $ process rawBytes
  where groupsCount = (header `shiftR` 1)
        byteCount :: Int
        byteCount = width * groupsCount
        process :: BS.ByteString -> [EncodedInt]
        process bs = reverse $ snd $ BS.foldl' extract initial bs
          where emptyBV = BV.bitVec 0 0
                initial = ((BV.bitVec 0 0), [])
                extract :: (BV.BitVector, [EncodedInt]) -> Word8 -> (BV.BitVector, [EncodedInt])
                extract (carry, acc) nextWord = if (BV.size combined) < width 
                    then (combined, acc)
                    else getAll (combined, acc)
                  where next :: BV.BitVector
                        next = BV.bitVec 8 nextWord
                        combined = BV.cat next carry
                        -- the integer of width at the start.
                        takeInt :: BV.BitVector -> EncodedInt
                        takeInt bv = fromIntegral $ BV.nat $ bv BV.@@ (width - 1, 0)
                        -- amount to shift the words by once an int is taken.
                        shiftAmount = (BV.bitVec width width)
                        -- the shifted result (if the int was taken).
                        shifted bv = if (BV.size bv) <= width then emptyBV else bv BV.@@ ((BV.size bv) - 1, width)
                        getAll :: (BV.BitVector, [EncodedInt]) -> (BV.BitVector, [EncodedInt])
                        getAll (current, acc) = 
                          if (BV.size current) < width
                            then (current, acc)
                            else getAll ((shifted current), (takeInt current) : acc)




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

