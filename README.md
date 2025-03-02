Implementez un simulator de tip BitTorrent utilizand MPI si pthreads.
Fisierele sunt impartite in segmente, iar fiecare client poate descarca
segmente lipsa de la alti clienti care le detin.
Trackerul pastreaza infromatii despre disponibilitatea fisierelor la clienti.
Clientii trimit odata la 10 descarcari actualizari catre tracker.

Clientul trimite catre tracker informatii despre fisierele pe care le detine
si requesturi pentru swarmurile fisierelor pe care vrea sa le descarce.
Trackerul trimite swarmurile fisierelor cerute de clienti si clientul cauta
segmente pe care nu le detine. Gaseste un segment si cere clientului care
detine acel segment. Il primeste si actualizeaza lista de segm detinute.
Daca un fisier e complet descarcat se scrie file-ul de output.
Clientul continua descarcarea pana cand are toate fisierele cerute.
Dupa descarcarea a 10 segmente clientii trimit un update catre tracker
iar trackerul actualizeaza swarmurile fisierelor.
