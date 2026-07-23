## Load Test Stage 1: Implementação Single Node, Concurrency em Contenção

Simula um fluxo de ordens realista (70% cancel, 20% add, 10% match) em
níveis crescentes de goroutines concorrentes, medindo throughput e
latência por operação individual.

Rodar com `go test -run TestLoad -v ./...`.

| Workers | Total de ops | Throughput | p50 | p99 |
|---|---|---|---|---|
| 1 | 500 | 2.301.867 ops/s | 248ns | 2,5µs |
| 10 | 5.000 | 1.207.425 ops/s | 384ns | 134µs |
| 50 | 25.000 | 1.055.594 ops/s | 476ns | 1,1ms |
| 100 | 50.000 | 1.137.860 ops/s | 438ns | 2,2ms |

### O que está acontecendo de verdade: contenção de lock

O `OrderBook` usa um único `sync.RWMutex` protegendo `AddOrder`, `Cancel` e
`Match`. Toda operação, por menor que seja, precisa esperar sua vez de
conseguir esse lock antes de fazer qualquer trabalho de verdade.

Com 1 worker, não tem ninguém pra esperar, então a latência fica baixa e
consistente. Assim que mais goroutines passam a disputar o mesmo lock,
duas coisas acontecem ao mesmo tempo:

- **O throughput cai na hora** (1 -> 10 workers: de 2,3M pra 1,2M ops/s),
  porque tempo de CPU que antes ia pra trabalho útil agora vai pra
  goroutines paradas, esperando o lock
- **O p99 cresce muito mais rápido que o p50** (2,5µs -> 2,2ms, quase
  900x, enquanto o p50 só sai de 248ns pra 438ns). A maioria das operações
  continua rápida assim que consegue o lock. Mas uma fatia crescente delas
  fica presa esperando atrás de todo mundo na fila, e são essas que
  inflam a cauda de latência

Esse é o formato clássico que a contenção de lock assume: a mediana quase
não se mexe, enquanto a cauda piora drasticamente. Um dashboard mostrando
só a latência média perderia esse padrão completamente.

### Por que o throughput estabiliza em vez de continuar subindo

Depois de 10 workers, o throughput fica relativamente estável (~1,0-1,1M
ops/s), em vez de continuar caindo ou subindo. Isso é o mutex já
totalmente saturado: o sistema já está gastando praticamente todo o tempo
que vai gastar serializando o acesso ao lock. Adicionar mais goroutines
depois desse ponto não ajuda nem atrapalha muito — só deixa a fila atrás
do lock mais comprida, que é exatamente o que o p99 crescente mostra.

### Uma ressalva que vale ser honesto sobre esses testes

`p50=248ns` pra uma operação que inclui busca numa skip list e mutação de
lista encadeada parece rápido demais. Explicação provável: com 70% das
operações sendo `Cancel` num ID sorteado aleatoriamente entre só 1.000
ordens pré-populadas, uma fração relevante dessas chamadas bate direto em
`if !exists { return false }`, sem fazer trabalho de verdade, porque outra
goroutine já cancelou aquele ID antes. Isso não invalida o padrão de
contenção (a fila do lock é real de qualquer forma), mas significa que o
número bruto de throughput está um pouco inflado. Um benchmark mais fiel
garantiria que cada cancel mira numa ordem que ainda está realmente ativa.

### Conclusão

A reescrita da estrutura de dados (skip list + lista duplamente encadeada)
fez o trabalho dela: operações individuais são rápidas. O teto atual não é
mais complexidade algorítmica, é o mutex único serializando todo o acesso.
Esse é o argumento concreto pra evolução planejada, seja particionando o
lock (ex: um lock por price level) ou indo pra um design estilo actor
(uma única goroutine dona do estado do book, todo mundo mais falando com
ela via channel, sem lock nenhum) conforme isso evolui pro matching engine
distribuído.