{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | This module provides an abstraction for implementing seek based
-- cursors by ID on keys using the "start at", "end before" and "limit"
-- parameters on arbitrary persistent entities and return types with a relation
-- to aforementioned entities
--
-- The cursor uses an ID key and primary ordering value and returns up to
-- "limit" number of rows whose primary ordering value is (>=) or (<=) the
-- primary ordering value of the passed in ID.  (depending on the order option
-- of ascending or descending).
--
-- There are a few major benefits of this style of pagination over the offset
-- limit style when used with a primary ordering value that monotonically
-- increases with time (such as `created_at`, or an incrementing `id`):
--
-- 1. Prevents skipped data if a destructive action occurs between one pagination request and the next
--      e.g. if you query for active cards with a limit offset and one is
--      cancelled you may end up skipping a card if you move onto the next
--      limit offset with the same limit and the offset as the previous offset + limit
-- 2. Prevents duplicated data if a new item is created between one pagination request and the next
--      e.g. if you query for transactions with a limit offset and a new
--      transaction is created when you go to the next page with the same limit
--      and the offset set to the previous offset + limit you'll repeat a
--      record
-- 3. When used on tables with `created_at` and id indexes is more efficient than limit offset pagination
--      Limit offset pagination will need to traverse the first n records in order to get to the offset
--      whereas the queries with cursor based pagination if properly indexed will be much more efficient
--      since the query planner will attempt to scan an existing index
--
-- This module can be used when we do selects on aggregate data that is associated
-- with some main record. A good example is the api transaction data aggregate
-- data used to make the response associated with a transaction metadata id
--
-- To take an existing function and refactor it to use this, refactor out the
-- DB query portion of the function (ie the part the queries for the data and
-- aggregates it) and parameterize the esqueleto portion after the table/from
-- clause.
--
-- That is change
--
-- @
--    myQuery :: m myValues
--    myQuery = do
--      ...
--        records <- from myTables
--        where_ (myPredicate records)
--        pure (mySelect records)
--      ...
-- @
--
-- to
--
-- @
--    myPagedQuery ::
--      (SqlExpr (Value primarySortValue) -> SqlExpr (Value recordId) -> SqlQuery ()) -> m myValues
--    myPagedQuery whenPageContains = do
--      ...
--        records <- from myTables
--        where_ (myPredicate records)
--        whenPageContains (myPrimarySortValueExpr records) (myRecordIdExpr records)
--        pure (mySelect records)
--      ...
-- @
--
-- The function must NOT reorder or enforce its own limit for the pagination to
-- work, though aggregation is allowed.
--
-- selectCursor is used to invoke the cursor function; so
--
-- @
--    allMyData <- myQuery
-- @
--
-- becomes
--
-- @
--    result <- selectCursor
--      MkCursorParams
--        { cursorQueryFn = myPagedQuery
--        , cursorConditionClause = CursorConditionWhere
--        , ...
--        }
--      myCursorParamsForCurrentPage
--
--    let onePageOfMyData = cursorData result
-- @
--
-- The `created_at` column is a good candidate for sorting a column because of
-- the aforementioned benefits and exists on most of our tables.
--
-- The cursor needs to know which record ID and primary sorting value to start
-- (or end) the page on (or after or before). The caller can choose to store
-- those values directly:
--
-- @
--    cursorResult <- selectCursor
--      MkCursorParams
--        { ...
--        , cursorCheckpoint = \selectedFields ->
--            ( myPrimarySortValue selectedFields
--            , myRecordId selectedFields
--            )
--        , cursorGetCheckpointData = \(primarySortValue, recordId) ->
--            pure $ Just (Value primarySortValue, Value recordId)
--        }
--      CursorParams
--        { cursorParamsCheckpoint = (myLastPrimarySortValue, myLastRecordId)
--        , cursorParamsType = StartAfterCursor
--        , ...
--        }
-- @
--
-- Or the user can store a *checkpoint* that can be used to recover the primary
-- sorting value and record ID before running the main query.
--
-- For instance, if the primary sorting value is a field of the keyed record,
-- that checkpoint could just be the record ID:
--
-- @
--    cursorResult <- selectCursor
--      MkCursorParams
--        { ...
--        , cursorCheckpoint = myRecordId
--        , cursorGetCheckpointData = \recordId -> do
--            record <- get recordId
--            pure (record.primarySortingField, Value recordId)
--        }
--      CursorParams
--        { cursorParamsCheckpoint = myLastRecordId
--        , cursorParamsType = StartAfterCursor
--        , ...
--        }
-- @
--
-- For examples of usage see `getCardPaymentDataCreatedAtCursor`,
-- `getMercuryAccountTransactionsCreatedAtCursor`, and
-- `getOrganizationAccountsData`
--
-- - Item keys that have the same primary column ordering are returned in ascending order by key if a conflict occurs (Arbitrary decision)
--      in order to maintain consistency between requests
-- - Each key is assumed to be associated with EXACTLY a single record (ie any joins done are assumed to have a unique constraint)
--      we do not enforce this in the types and it is up to the caller to make this guarantee
module Database.Esqueleto.SeekPagination (
  CursorParams (..),
  CursorType (..),
  CursorException (..),
  CursorResult (..),
  MkCursorParams (..),
  selectCursor,
  getValueAndKeyFromEntity,
  withValueAndKeyFromEntity,
  CursorConditionClause (..),
)
where

import Control.Monad.IO.Class
import Control.Monad.Trans.Class
import Control.Monad.Trans.Maybe
import Control.Monad.Trans.Reader
import Data.Int
import Data.Maybe
import Database.Esqueleto.Experimental
import qualified Database.Esqueleto.Experimental as DB
import qualified Database.Esqueleto.Internal.Internal as DB
import Safe

data OrderDirection = OrderAsc | OrderDesc
  deriving (Eq)

data NullDirection = NullsFirst | NullsLast

data NullableOrderDirection
  = NullableOrderDirection OrderDirection NullDirection

-- | Whether this cursor is used as a WHERE clause or a HAVING clause
data CursorConditionClause = CursorConditionWhere | CursorConditionHaving

data CursorType
  = -- | The results should start with the checkpointed value and continue after it (INCLUSIVE)
    StartAtCursor
  | -- | The results should start just after the checkpointed value (EXCLUSIVE)
    StartAfterCursor
  | -- | The results should end just before the checkpointed value (EXCLUSIVE)
    EndBeforeCursor

data CursorParams checkpoint = CursorParams
  { cursorParamsCheckpoint :: Maybe checkpoint
  , cursorParamsLimit :: Int64
  , cursorParamsType :: CursorType
  , cursorParamsPrimaryFieldOrder :: NullableOrderDirection
  }

newtype CursorException checkpoint = CursorKeyNotFoundException checkpoint

data CursorResult checkpoint sqlReturnType = CursorResult
  { cursorStart :: Maybe checkpoint
  -- ^ Cursor start should be the first element of cursor data.
  -- You can move backwards by using `EndBeforeCursor` and `cursorStart` as the `cursorParamsKey`
  , cursorEnd :: Maybe checkpoint
  -- ^ Cursor end should be the last element of cursor data
  -- You can move forwards by using the `StartAfterCursor` and `cursorEnd` as the `cursorParamsKey`
  , cursorNextStartAt :: Maybe checkpoint
  -- ^ The next id for the current cursor direction
  -- You can move forwards by using the `StartAtCursor` and `cursorNextStartAt` as the `cursorParamsKey`
  , cursorCount :: Int
  , cursorHasMore :: Bool
  -- ^ If there is another page in the direction of the pagination
  -- (ex: If using `EndBeforeCursor`, cursorHasMore = True means there is another previous page.
  -- If using `StartAfterCursor` or `StartAtCursor`, cursorHasMore = True means there is another nextPage)
  , cursorData :: [sqlReturnType]
  }

-- | A cursor can return an arbitrary data type, typically from the associated
-- associated with some base persistent type joined with other tables. (For
-- example an api transaction response will be associated with
-- transaction_metadata, and built from accounts, transaction parties etc.)
--
-- Queries are sorted by some per-row value and record id. Particular (value,id)
-- pairs are then used to delimit the pages found by the cursor.  The user is
-- not required to store this tuple between page requests directly, but may
-- instead store a checkpoint computed from a returned row that can later be
-- used to reconstruct the (value, id) pair for that row.
data MkCursorParams m checkpoint row = forall a r.
  (PersistField a, PersistEntity r) =>
  MkCursorParams
  { cursorCheckpoint :: row -> checkpoint
  -- ^ Conversion from a particular returned row to the checkpoint for that
  -- row.
  , cursorGetCheckpointData :: checkpoint -> m (Maybe (Value a, Value (Key r)))
  -- ^ A function called before query time to recover the (value, id) pair for a
  --  row used to identify the requested page from the checkpoint.
  , cursorQueryFn :: (SqlExpr (Value a) -> SqlExpr (Value (Key r)) -> SqlQuery ()) -> m [row]
  -- ^ A function which will build the return type and which accepts an
  -- esqueleto sql expression with an entity as a parameter for pagination
  -- - Assumes that a function which builds sql clause from the selected table
  --   can be taken as a parameter.
  -- - Assumes that no order by or limit clauses are provided by this function
  --    The pagination will not work if such clauses are provided or if the function re-orders or enforces its own limit
  --    in any other way
  -- - Assumes that each key is associated with EXACTLY a single record (ie any joins done are assumed to have a unique constraint)
  --      we do not enforce this since we accept an arbitrary return type and it is up to the caller to make this guarantee
  --
  -- See `getCardPaymentDataPaginated` and `getMercuryAccountTransactionsPaginated` for examples
  , cursorConditionClause :: CursorConditionClause
  -- ^ whether the query is to be used as 'WHERE' or 'HAVING'
  }

-- | 'cursorGetCheckpointData' helper for the common case when using checkpoint ~ Key sqlEntity
getValueAndKeyFromEntity ::
  forall sqlEntity a m backend.
  ( PersistField a
  , PersistEntity sqlEntity
  , SymbolToField "id" sqlEntity (Key sqlEntity)
  , MonadIO m
  , SqlBackendCanRead backend
  ) =>
  (SqlExpr (Entity sqlEntity) -> SqlExpr (Value a)) ->
  Key sqlEntity ->
  ReaderT backend m (Maybe (Value a, Value (Key sqlEntity)))
getValueAndKeyFromEntity selector paramKey = DB.selectOne do
  sqlEntity <- from $ table @sqlEntity
  where_ $ sqlEntity ^. #id ==. val paramKey
  pure (selector sqlEntity, sqlEntity ^. #id)

-- | 'cursorQueryFn' helper for the common case when using checkpoint ~ Key sqlEntity
withValueAndKeyFromEntity ::
  ( SymbolToField "id" sqlEntity (Key sqlEntity)
  , PersistEntity sqlEntity
  ) =>
  (SqlExpr (Entity sqlEntity) -> SqlExpr (Value a)) ->
  (SqlExpr (Value a) -> SqlExpr (Value (Key sqlEntity)) -> r) ->
  (SqlExpr (Entity sqlEntity) -> r)
withValueAndKeyFromEntity selector k sqlEntity = k (selector sqlEntity) (sqlEntity ^. #id)

-- | A function which builds the return type
-- - Assumes that a function which builds sql clause from the selected table
--   can be taken as a parameter.

-- Performs two queries to build a cursor result but assuming an index exists on
-- the primary ordering column for the base entity in question this will be more
-- efficient for large datasets than a limit offset query since we dont need to worry
-- about fetching all the records.
-- Query (1) Gets the primary ordering key of the passed in cursor ID
-- Query (2) Gets all rows whose primary ordering value is (=) the primary
--      ordering value of the passed in ID whose IDs are > the cursor ID's row
--      AND all the rows whose ordering value is (>) if ascending or (<) if
--      descending order than the cursor ID's
--
-- requires a function which builds the aggregate data type via esqueleto
-- select and accepts sql clauses.
selectCursor ::
  forall checkpoint row m backend.
  ( Eq checkpoint
  , MonadIO m
  , SqlBackendCanRead backend
  ) =>
  MkCursorParams (ReaderT backend m) checkpoint row ->
  CursorParams checkpoint ->
  ReaderT backend m (CursorResult checkpoint row)
selectCursor (MkCursorParams {..}) (CursorParams {..}) = do
  -- We use a larger limit so we can return the "has more" field and next cursor
  let limitWithStartAtNext =
        case cursorParamsType of
          StartAtCursor -> cursorParamsLimit + 1
          -- we drop the first with start after so next would be at +2 rather than +1 if a key is passed in
          StartAfterCursor ->
            case cursorParamsCheckpoint of
              Nothing -> cursorParamsLimit + 1
              Just _ -> cursorParamsLimit + 2
          EndBeforeCursor ->
            case cursorParamsCheckpoint of
              Nothing -> cursorParamsLimit + 1
              Just _ -> cursorParamsLimit + 2
      -- NOTE: an end at cursor is functionally just a start at cursor with FULLY REVERSED ORDER.
      --   and not inclusive of the cursorCheckpoint. (In other words cursor orderings
      --   operators for endBefore should be the reverse of startAfterCursor). We reverse the order
      --   in the end before case before returning the result
      compareIdOp =
        case cursorParamsType of
          StartAtCursor -> (<=.)
          StartAfterCursor -> (<=.)
          EndBeforeCursor -> (>=.)
      keySortOrder =
        case cursorParamsType of
          StartAtCursor -> desc
          StartAfterCursor -> desc
          EndBeforeCursor -> asc
      startPrimaryColOrderOp =
        case cursorParamsPrimaryFieldOrder of
          NullableOrderDirection OrderAsc _ -> (>.)
          NullableOrderDirection OrderDesc _ -> (<.)
      comparePrimaryOrderColOp =
        case cursorParamsType of
          StartAtCursor -> startPrimaryColOrderOp
          StartAfterCursor -> startPrimaryColOrderOp
          EndBeforeCursor ->
            case cursorParamsPrimaryFieldOrder of
              NullableOrderDirection OrderAsc _ -> (<.)
              NullableOrderDirection OrderDesc _ -> (>.)
      conditionClause = case cursorConditionClause of
        CursorConditionWhere -> where_
        CursorConditionHaving -> having
      startSortOrder :: (PersistField a) => SqlExpr (Value a) -> SqlExpr OrderBy
      startSortOrder =
        case cursorParamsPrimaryFieldOrder of
          NullableOrderDirection OrderAsc NullsLast -> asc
          NullableOrderDirection OrderAsc NullsFirst -> ascNullsFirst
          NullableOrderDirection OrderDesc NullsFirst -> desc
          NullableOrderDirection OrderDesc NullsLast -> descNullsLast

      primarySortOrder :: (PersistField a) => SqlExpr (Value a) -> SqlExpr OrderBy
      primarySortOrder =
        case cursorParamsType of
          StartAtCursor -> startSortOrder
          StartAfterCursor -> startSortOrder
          EndBeforeCursor ->
            case cursorParamsPrimaryFieldOrder of
              NullableOrderDirection OrderAsc NullsLast -> desc
              NullableOrderDirection OrderAsc NullsFirst -> descNullsLast
              NullableOrderDirection OrderDesc NullsFirst -> asc
              NullableOrderDirection OrderDesc NullsLast -> ascNullsFirst

      emptyResult =
        CursorResult
          { cursorStart = Nothing
          , cursorEnd = Nothing
          , cursorNextStartAt = Nothing
          , cursorCount = 0
          , cursorHasMore = False
          , cursorData = []
          }

      cursorOrderClause primaryOrderField sqlEntityId =
        orderBy
          [ primarySortOrder primaryOrderField
          , keySortOrder sqlEntityId
          ]

  -- Default to the first key by the ordering if none is passed in

  res <- runMaybeT $ do
    queryResult :: [row] <- case cursorParamsCheckpoint of
      Nothing ->
        lift $ cursorQueryFn $ \primaryOrderField sqlEntityId -> do
          cursorOrderClause primaryOrderField sqlEntityId
          limit limitWithStartAtNext
          pure ()
      Just paramCheckpoint -> do
        (Value comparePrimaryOrderField, Value compareSqlEntityId) <- MaybeT do
          cursorGetCheckpointData paramCheckpoint

        let sqlClauses primaryOrderField sqlEntityId = do
              -- Fetch rows whose primary col is the same value as the primary col of the cursor key
              -- and whose id is (>=.) in the start at case or (<=.) in the end
              -- before case of the passed in key
              conditionClause $
                ( (sqlEntityId `compareIdOp` val compareSqlEntityId)
                    &&. (primaryOrderField ==. val comparePrimaryOrderField)
                )
                  -- Fetch rows whose primary col value is (>) or (<) the cursor key based on the cursor order
                  ||. ( primaryOrderField
                          `comparePrimaryOrderColOp` val comparePrimaryOrderField
                      )
              cursorOrderClause primaryOrderField sqlEntityId
              limit limitWithStartAtNext

        -- The query result will include at most limit+1 items. We use the larger limit to determine
        -- if there are any remaining records after the current cursor and drop any extra
        -- records returned
        queryResult :: [row] <- lift $ do
          resNotNullable <- cursorQueryFn sqlClauses
          -- Workaround for if the primary comparison value is null (and we can
          -- therefor not use comparison operators with it, but otherwise dont
          -- need to worry about primary col collisions since nulls will be
          -- ordered either at the start or end)
          if null resNotNullable
            then
              cursorQueryFn
                ( \primaryOrderField sqlEntityId -> do
                    conditionClause (sqlEntityId `compareIdOp` val compareSqlEntityId)
                    cursorOrderClause primaryOrderField sqlEntityId
                    limit limitWithStartAtNext
                )
            else pure resNotNullable
        pure queryResult

    let limitedResult =
          case cursorParamsType of
            -- Take all but the next cursor
            StartAtCursor -> take (fromIntegral cursorParamsLimit) queryResult
            StartAfterCursor ->
              case cursorParamsCheckpoint of
                -- Start After should be the same as start at if no key is put in (going from the first element)
                Nothing -> take (fromIntegral cursorParamsLimit) queryResult
                Just _ ->
                  -- Drop the cursor if it is included
                  if (cursorCheckpoint <$> headMay queryResult) == cursorParamsCheckpoint
                    then take (fromIntegral cursorParamsLimit) $ drop 1 queryResult
                    else take (fromIntegral cursorParamsLimit) queryResult
            EndBeforeCursor ->
              case cursorParamsCheckpoint of
                -- Drop the input cursor if it is included and reverse the result to be consistent
                -- with the start at cursor
                Nothing -> take (fromIntegral cursorParamsLimit) $ reverse queryResult
                Just _ ->
                  if (cursorCheckpoint <$> headMay queryResult) == cursorParamsCheckpoint
                    then reverse $ take (fromIntegral cursorParamsLimit) $ drop 1 queryResult
                    else reverse $ take (fromIntegral cursorParamsLimit) queryResult
        nextStartAt =
          case cursorParamsType of
            StartAtCursor -> cursorCheckpoint <$> (queryResult `atMay` fromIntegral cursorParamsLimit)
            StartAfterCursor ->
              case cursorParamsCheckpoint of
                -- Start After should be the same as start at if no key is put in (turning from the first element)
                Nothing -> cursorCheckpoint <$> (queryResult `atMay` fromIntegral cursorParamsLimit)
                Just _ -> cursorCheckpoint <$> (queryResult `atMay` fromIntegral (cursorParamsLimit + 1))
            -- the next of a end before will always be the passed in key
            EndBeforeCursor -> cursorParamsCheckpoint
        hasMore = case cursorParamsType of
          StartAtCursor -> isJust nextStartAt
          StartAfterCursor -> isJust nextStartAt
          EndBeforeCursor ->
            case cursorParamsCheckpoint of
              Nothing -> isJust (queryResult `atMay` fromIntegral cursorParamsLimit)
              Just _ -> isJust (queryResult `atMay` fromIntegral (cursorParamsLimit + 1))
        result =
          CursorResult
            { cursorStart = cursorCheckpoint <$> headMay limitedResult
            , cursorEnd = cursorCheckpoint <$> lastMay limitedResult
            , cursorNextStartAt = nextStartAt
            , cursorCount = length limitedResult
            , cursorHasMore = hasMore
            , cursorData = limitedResult
            }
    pure result

  maybe (pure emptyResult) pure res

-- | Ascending order of this field or SqlExpression with nulls first.
ascNullsFirst :: (PersistField a) => SqlExpr (Value a) -> SqlExpr OrderBy
ascNullsFirst = DB.orderByExpr " ASC NULLS FIRST"

-- | Descending order of this field or SqlExpression with nulls last.
descNullsLast :: (PersistField a) => SqlExpr (Value a) -> SqlExpr OrderBy
descNullsLast = DB.orderByExpr " DESC NULLS LAST"
