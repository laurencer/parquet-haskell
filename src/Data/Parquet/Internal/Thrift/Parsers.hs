
module Data.Parquet.Internal.Thrift.Parsers(
    FieldId
  , ParsedStruct 
  , ThriftParseable(..)
  , readField
  , asInt
  , asMaybeInt
  , asMaybeString
  , asMaybeList
  , asStruct
  , asList
  , required
  , asEnum
  , asMaybeEnum
  , asMaybeBool
  , asParsedList
  , asMaybeEnumList
  , asMaybeParsedList
  , asMaybeParseThrift
  ) where

import Control.Applicative
import Control.Monad

import Data.Hashable ( Hashable, hashWithSalt )
import Data.Int
import Data.Text.Lazy (Text)
import qualified Data.ByteString.Lazy as LBS
import qualified Data.HashMap.Strict as Map
import qualified Data.HashSet as Set
import qualified Data.Vector as Vector

import Data.Attoparsec.Internal.Types

import Data.Parquet.Internal.Thrift.Types

type FieldId = Int16
type ParsedStruct = Map.HashMap Int16 (Text, ThriftVal)

class ThriftParseable a where 
  parseThrift :: ThriftVal -> Parser i a

instance ThriftParseable (LBS.ByteString) where
  parseThrift (TString s) = return s
  parseThrift _ = wrongFieldType 

instance ThriftParseable Int where
  parseThrift (TI16 i) = return $ fromIntegral i
  parseThrift (TI32 i) = return $ fromIntegral i
  parseThrift (TI64 i) = return $ fromIntegral i
  parseThrift _ = wrongFieldType

instance ThriftParseable Int16 where
  parseThrift (TI16 i) = return $ fromIntegral i
  parseThrift _ = wrongFieldType

instance ThriftParseable Int32 where
  parseThrift (TI32 i) = return $ fromIntegral i
  parseThrift _ = wrongFieldType

instance ThriftParseable Int64 where
  parseThrift (TI64 i) = return $ fromIntegral i
  parseThrift _ = wrongFieldType

get :: (Eq k, Hashable k) => k -> Map.HashMap k v -> Maybe v
get k m = if (Map.member k m) then Just (m Map.! k) else Nothing

getValue :: (Eq k, Hashable k) => k -> Map.HashMap k (r, v) -> Maybe v
getValue k m = snd <$> (get k m)

wrongFieldType :: Parser i a
wrongFieldType = fail "Wrong field type"

fieldMissingError :: Parser i a
fieldMissingError = fail "Field missing"

readField :: ParsedStruct -> FieldId -> Parser i (Maybe ThriftVal)
readField p fieldId = return $ getValue fieldId p

asInt :: (Maybe ThriftVal) -> Parser i Int
asInt (Just (TI16 i)) = return $ fromIntegral i
asInt (Just (TI32 i)) = return $ fromIntegral i
asInt (Just (TI64 i)) = return $ fromIntegral i
asInt Nothing = fieldMissingError
asInt _ = wrongFieldType

asMaybeInt :: (Maybe ThriftVal) -> Parser i (Maybe Int)
asMaybeInt (Just (TI16 i)) = return $ Just (fromIntegral i)
asMaybeInt (Just (TI32 i)) = return $ Just (fromIntegral i)
asMaybeInt (Just (TI64 i)) = return $ Just (fromIntegral i)
asMaybeInt Nothing = return Nothing
asMaybeInt _ = wrongFieldType

asMaybeEnum :: Enum a => (Maybe ThriftVal) -> Parser i (Maybe a)
asMaybeEnum mtv = (asMaybeInt mtv) >>= (\x -> return $ toEnum <$> x)

asMaybeEnumList :: Enum a => (Maybe ThriftVal) -> Parser i (Maybe [a])
asMaybeEnumList mtv = (\x -> convertToEnums <$> x) <$> maybeIntList
  where maybeIntList :: Parser i (Maybe [Int])
        maybeIntList = asMaybeParsedList mtv
        convertToEnums is = toEnum <$> is

asEnum :: Enum a => (Maybe ThriftVal) -> Parser i a
asEnum mtv = (asInt mtv) >>= (\x -> return $ toEnum x)

asMaybeString :: (Maybe ThriftVal) -> Parser i (Maybe LBS.ByteString)
asMaybeString (Just (TString v)) = return $ Just v
asMaybeString Nothing = return Nothing
asMaybeString _       = wrongFieldType

asMaybeBool :: (Maybe ThriftVal) -> Parser i (Maybe Bool)
asMaybeBool (Just (TBool b)) = return $ Just b
asMaybeBool Nothing = return Nothing
asMaybeBool _       = wrongFieldType

asStruct :: ThriftVal -> Parser i (ParsedStruct)
asStruct (TStruct p) = return p
asStruct _ = wrongFieldType

asList :: ThriftVal -> Parser i [ThriftVal]
asList (TList _ vs) = return vs
asList _ = wrongFieldType

asMaybeParseThrift :: ThriftParseable a => (Maybe ThriftVal) -> Parser i (Maybe a)
asMaybeParseThrift (Just mtv) = Just <$> (parseThrift mtv)
asMaybeParseThrift Nothing    = return Nothing

asMaybeList :: (Maybe ThriftVal) -> Parser i (Maybe [ThriftVal])
asMaybeList (Just (TList _ vs)) = return $ Just vs
asMaybeList Nothing = return $ Nothing
asMaybeList _ = wrongFieldType

asParsedList :: ThriftParseable a => ThriftVal -> Parser i [a]
asParsedList tv = asList tv >>= (\x -> sequence $ map parseThrift x)

asMaybeParsedList :: ThriftParseable a => (Maybe ThriftVal) -> Parser i (Maybe [a])
asMaybeParsedList tv = asMaybeList tv >>= parse
  where parse :: ThriftParseable a => (Maybe [ThriftVal]) -> Parser i (Maybe [a])
        parse (Just tvs) = Just <$> (sequence $ map parseThrift tvs)
        parse Nothing = return $ Nothing

required :: Maybe a -> Parser i a
required (Just v) = return v
required Nothing = fieldMissingError
