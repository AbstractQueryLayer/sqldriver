<?php

declare(strict_types=1);

namespace IfCastle\AQL\SqlDriver;

use IfCastle\AQL\Dsl\BasicQueryInterface;
use IfCastle\AQL\Dsl\Sql\Query\QueryInterface;
use IfCastle\AQL\Entity\EntityInterface;
use IfCastle\AQL\Executor\Ddl\DdlExecutorFactoryInterface;
use IfCastle\AQL\Executor\Ddl\DdlExecutorForSql;
use IfCastle\AQL\Executor\Ddl\DdlExecutorInterface;
use IfCastle\AQL\Executor\QueryExecutorInterface;
use IfCastle\AQL\Executor\QueryExecutorResolverInterface;
use IfCastle\AQL\Executor\SqlQueryExecutor;
use IfCastle\AQL\Generator\Ddl\EntityToTableFactoryInterface;
use IfCastle\AQL\Result\ResultInterface;
use IfCastle\AQL\Storage\ConnectableTelemetryInterface;
use IfCastle\AQL\Storage\Exceptions\ConnectFailed;
use IfCastle\AQL\Storage\Exceptions\DuplicateKeysException;
use IfCastle\AQL\Storage\Exceptions\QueryException;
use IfCastle\AQL\Storage\Exceptions\RecoverableException;
use IfCastle\AQL\Storage\Exceptions\ServerHasGoneAwayException;
use IfCastle\AQL\Storage\Exceptions\StorageException;
use IfCastle\AQL\Storage\Exceptions\TransactionResumeException;
use IfCastle\AQL\Storage\QueryableTelemetryInterface;
use IfCastle\AQL\Storage\SqlStatementFactoryInterface;
use IfCastle\AQL\Storage\SqlStatementInterface;
use IfCastle\AQL\Storage\SqlStorageInterface;
use IfCastle\AQL\Transaction\TransactionAbleInterface;
use IfCastle\AQL\Transaction\TransactionAwareInterface;
use IfCastle\AQL\Transaction\TransactionInterface;
use IfCastle\DI\AutoResolverInterface;
use IfCastle\DI\ContainerInterface;
use IfCastle\DI\DisposableInterface;
use IfCastle\DI\Exceptions\ConfigException;

abstract class SqlDriverAbstract implements
    SqlStorageInterface,
    TransactionAbleInterface,
    SqlStatementFactoryInterface,
    EntityToTableFactoryInterface,
    DdlExecutorFactoryInterface,
    QueryExecutorResolverInterface,
    AutoResolverInterface,
    DisposableInterface
{
    protected ?string $storageName  = null;

    protected string $dsn;

    protected ?string $username     = null;

    protected ?string $password     = null;

    /**
     * @var array<string, string>
     */
    protected array $options;

    protected string $initialQueries    = '';

    protected int $reconnectAttempts    = 0;

    protected int $maxAttempts          = 3;

    protected ?TransactionInterface $transaction = null;

    protected array $transactionStack   = [];

    protected ?StorageException $lastError = null;

    protected ConnectableTelemetryInterface|null $telemetry = null;

    protected QueryableTelemetryInterface|null $queryTelemetry = null;

    /**
     * @throws ConfigException
     */
    public function __construct(array $config)
    {
        if (!\array_key_exists(self::OPTIONS, $config)) {
            $config[self::OPTIONS] = [];
        }

        if (!\array_key_exists('dsn', $config)) {
            throw new ConfigException('dsn is not defined');
        }

        foreach (['dsn', 'username', 'password', self::OPTIONS] as $name) {
            $this->$name            = $config[$name] ?? null;
        }

        if (!empty($config[self::MAX_ATTEMPTS])) {
            $this->maxAttempts      = (int) $config[self::MAX_ATTEMPTS];
        }

        if ($this->maxAttempts === 0) {
            $this->maxAttempts      = 1;
        }

        if (!empty($config[self::INITIAL_QUERIES])) {
            $this->initialQueries   = (string) $config[self::INITIAL_QUERIES];
        }
    }

    #[\Override]
    public function resolveDependencies(ContainerInterface $container): void
    {
        $this->telemetry             = $container->findDependency(ConnectableTelemetryInterface::class);
        $this->queryTelemetry        = $container->findDependency(QueryableTelemetryInterface::class);
    }

    /**
     * @throws ConnectFailed
     */
    #[\Override]
    public function connect(): void
    {
        $exception                  = null;
        $lastException              = null;

        do {
            try {
                $this->connectionAttempt();
                $this->reconnectAttempts = 0;
                break;
            } catch (ConnectFailed $lastException) {
                ++$this->reconnectAttempts;
            }
        } while ($this->reconnectAttempts < $this->maxAttempts);

        if ($exception !== null) {
            throw $exception;
        }

        if ($this->isDisconnected()) {
            throw new ConnectFailed('Connection failed after ' . $this->reconnectAttempts . ' attempts', 0, $lastException);
        }

        if ($this->initialQueries !== '') {
            $this->realExecuteQuery($this->initialQueries);
        }
    }

    abstract protected function connectionAttempt(): void;

    abstract protected function realExecuteQuery(string $sql): ResultInterface;

    /**
     * @throws ConnectFailed
     * @throws RecoverableException
     * @throws DuplicateKeysException
     * @throws QueryException
     * @throws ServerHasGoneAwayException
     * @throws StorageException
     */
    #[\Override]
    public function executeSql(string $sql, ?object $context = null): ResultInterface
    {
        if ($this->isDisconnected()) {
            $this->connect();
        }

        $transaction                = $context instanceof TransactionAwareInterface ? $context->getTransaction() : null;

        if ($transaction !== null && false === $transaction->isTransactionOpened($this->getStorageName())) {
            $this->beginTransaction($transaction);
        }

        return $this->realExecuteQuery($sql);
    }

    abstract protected function realCreateStatement(string $sql): SqlStatementInterface;

    #[\Override]
    public function createStatement(string $sql, ?object $context = null): SqlStatementInterface
    {
        if ($this->isDisconnected()) {
            $this->connect();
        }

        return $this->realCreateStatement($sql);
    }

    abstract protected function realExecuteStatement(SqlStatementInterface $statement): ResultInterface;

    #[\Override]
    public function executeStatement(SqlStatementInterface $statement, array $params = [], ?object $context = null): ResultInterface
    {
        if ($this->isDisconnected()) {
            $this->connect();
        }

        $transaction                = $context instanceof TransactionAwareInterface ? $context->getTransaction() : null;

        if ($transaction !== null && false === $transaction->isTransactionOpened($this->getStorageName())) {
            $this->beginTransaction($transaction);
        }



        return $this->realExecuteStatement($statement);
    }

    /**
     * @throws ConnectFailed
     */
    #[\Override]
    public function beginTransaction(TransactionInterface $transaction): void
    {
        if ($this->isDisconnected()) {
            $this->connect();
        }

        if ($this->transaction !== null && $transaction->getParentTransaction() === null) {
            $transaction->setParentTransaction(
                $transaction
            );
        }

        $this->transactionStack[]   = $transaction;
        $this->transaction          = $transaction;

        $transaction->openTransaction($this->getStorageName(), $this->finalizeTransaction(...));

        $this->realBeginTransaction($transaction);
    }

    abstract protected function isDisconnected(): bool;

    abstract protected function realBeginTransaction(TransactionInterface $transaction): void;

    #[\Override]
    public function getTransaction(): ?TransactionInterface
    {
        return $this->transaction;
    }

    /**
     * @throws ConnectFailed
     */
    #[\Override]
    public function quote(mixed $value): string
    {
        if ($this->isDisconnected()) {
            $this->connect();
        }

        if (\is_bool($value)) {
            return $value ? 'TRUE' : 'FALSE';
        }

        if (\is_int($value) || \is_float($value) || \is_null($value)) {
            return (string) $value;
        }

        if ($value instanceof \Stringable) {
            $value                  = (string) $value;
        } elseif ($value instanceof \DateTimeInterface) {
            $value                  = $value->format('Y-m-d H:i:s');
        }

        return $this->realQuote($value);
    }

    abstract protected function realQuote(string $value): string;

    #[\Override]
    public function escape(string $value): string
    {
        // Only for MySQL and SQLite
        return '`' . $value . '`';
    }

    /**
     * @throws ConnectFailed
     */
    #[\Override]
    public function lastInsertId(): string|int|float|null
    {
        if ($this->isDisconnected()) {
            $this->connect();
        }

        $id                         = $this->realLastInsertId();

        if ($id === false) {
            return null;
        }

        // Try to cast string to int
        if (\is_string($id) && (int) $id == $id) {
            return (int) $id;
        }

        return $id;
    }

    abstract protected function realLastInsertId(): mixed;

    #[\Override]
    public function getLastError(): ?StorageException
    {
        return $this->lastError;
    }

    #[\Override]
    public function disconnect(): void
    {
        $this->telemetry?->registerDisconnect($this);
    }

    #[\Override]
    public function getStorageName(): ?string
    {
        return $this->storageName;
    }

    #[\Override]
    public function setStorageName(string $storageName): static
    {
        $this->storageName          = $storageName;

        return $this;
    }

    #[\Override]
    public function resolveQueryExecutor(BasicQueryInterface $basicQuery, ?EntityInterface $entity = null): ?QueryExecutorInterface
    {
        return match ($basicQuery->getQueryAction()) {
            QueryInterface::ACTION_COPY,
            QueryInterface::ACTION_SELECT,
            QueryInterface::ACTION_COUNT,
            QueryInterface::ACTION_INSERT,
            QueryInterface::ACTION_UPDATE,
            QueryInterface::ACTION_DELETE,
            QueryInterface::ACTION_REPLACE
                                    => new SqlQueryExecutor(),
            default                 => null
        };
    }

    #[\Override]
    public function newDdlExecutor(string $entityName): DdlExecutorInterface
    {
        return new DdlExecutorForSql($entityName);
    }

    #[\Override]
    public function dispose(): void
    {
        $this->disconnect();
    }

    protected function finalizeTransaction(bool $status, TransactionInterface $transaction): void
    {
        if ($this->transaction !== $transaction) {
            return;
        }

        if ($this->transactionStack !== [] && false === $this->isNestedTransactionsSupported()) {

            if ($status) {
                $this->transaction  = \array_pop($this->transactionStack);
                return;
            }

            // If nested transactions not supported, then we must rollback all transactions
            $this->transaction  = null;
            $exception          = null;

            try {
                while ($this->transactionStack !== []) {
                    $transaction    = \array_pop($this->transactionStack);
                    $transaction->rollBack();
                }
            } catch (TransactionResumeException) {

                // Special case, when the transaction was resumed
                // So we restore the last transaction and continue.
                $this->transaction  = $transaction;

                return;

            } catch (\Throwable $exception) {

            }

            $this->realRollback($transaction);

            if ($exception !== null) {
                throw $exception;
            }

            return;
        }

        if ($this->transactionStack !== []) {
            $this->transaction      = \array_pop($this->transactionStack);
        }

        if ($status) {
            $this->realCommit($transaction);
        } else {
            $this->realRollback($transaction);
        }
    }

    abstract protected function realCommit(TransactionInterface $transaction): void;

    abstract protected function realRollback(TransactionInterface $transaction): void;

    abstract protected function normalizeException(\Throwable $exception, string $sql): StorageException;

    abstract protected function isNestedTransactionsSupported(): bool;
}
