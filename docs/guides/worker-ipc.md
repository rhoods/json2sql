---
type: carte
sujet: Worker de l'IHM et protocole IPC (config, socket, fichier résultat)
verified_at: 98969c6
files:
  - json2sql-worker/src/
  - src/ipc.rs
  - src/io/progress_event.rs
  - src/io/progress.rs
  - json2sql-ui/src/worker_client.rs
  - json2sql-ui/src/screens/import.rs
  - json2sql-ui/src/screens/resume.rs
  - json2sql-ui/src/state/mod.rs
  - json2sql-ui/src/main.rs
  - docs/technical/architecture.md
  - docs/technical/modules.md
symbols:
  - WorkerConfig
  - WorkerConfig::into_pass2_config
  - WorkerCommand
  - WorkerResult
  - new_socket_path
  - lockfile_path
  - ProgressEvent
  - ProgressTracker
  - LockfileGuard
  - LockfileGuard::try_acquire
  - spawn_signal_cancel
  - bind_socket
  - write_result
  - run_pipeline
  - build_pg_url
  - run_interruptible
  - load_and_constrain
  - serve_connections
  - handle_connection
  - ImportSummary
  - ImportSummary::push
  - CancelToken
  - CancelToken::cancelled
  - spawn_worker
  - connect_with_retry
  - is_lockfile_free
  - find_active_socket
  - WorkerHandle
  - WorkerKillHandle
  - WorkerKillHandle::terminate
  - SocketEventReader
  - SocketEventReader::next_event
  - ImportScreen
  - ResumeScreen
  - AnalysisScreen
  - AppState::apply_progress_event
  - AppState::apply_worker_result
  - create_tables_no_constraints
  - add_constraints
  - emit_completion_events
  - Pass2Config
  - Pass1Progress
  - Pass2Progress
---

# Worker et IPC

Carte du processus `json2sql-worker` et du protocole qui le relie à l'IHM, pour savoir où intervenir et ce qui casse
sans erreur de compilation. Présentation courte du protocole : [architecture.md § src/ipc.rs](../technical/architecture.md#srcipcrs) ;
les événements et leurs données : [architecture.md § progress_event.rs](../technical/architecture.md#progress_eventrs) ;
un fichier par ligne : [modules.md § json2sql-worker/src/](../technical/modules.md#json2sql-workersrc).

## Rôle

L'IHM ne lance pas l'import elle-même : elle démarre un processus `json2sql-worker`, lui envoie la config
(`WorkerConfig`, schémas déjà finalisés) et suit l'import sur un socket Unix. Le worker survit à la fermeture de l'IHM :
une IHM relancée retrouve le socket et reprend le suivi. Le worker ne fait **jamais** de Pass 1 (l'IHM la fait
elle-même, voir [pass1.md](pass1.md)) ; son import, de `run_pipeline` à `run_interruptible`, est décrit dans
[pass2.md](pass2.md).

Trois canaux, chacun dans un seul sens :

- **stdin** (IHM → worker) : `WorkerConfig` en JSON, une fois, puis fermé ; le mot de passe PostgreSQL passe à part,
  dans la variable d'environnement `J2S_PG_PASSWORD` ;
- **socket Unix** : du worker vers l'IHM, les `ProgressEvent`, un par ligne JSON ; de l'IHM vers le worker, une
  commande `WorkerCommand` (aujourd'hui seulement `{"cmd":"cancel"}`) ;
- **fichier résultat** (worker → IHM) : `WorkerResult`, écrit à la fin, lu par l'IHM seulement si le socket s'est fermé
  avant `Pass2Done`.

`worker_client.rs` fait partie de l'IHM mais il est décrit ici : c'est le bout IHM du protocole. Ce que l'IHM fait des
événements (écrans, barres) relève de la carte IHM.

## 1. Où intervenir

| Changement | Où | Le compilateur aide ? |
|---|---|---|
| Nouveau réglage de l'import envoyé par l'IHM | `WorkerConfig`, rempli dans `ImportScreen` ; lu dans `run_pipeline` ou `WorkerConfig::into_pass2_config` (§ 4) | oui pour le remplir, **non** pour le lire |
| Nouvel événement de progression | `ProgressEvent`, son émission, `AppState::apply_progress_event` (§ 4) | oui pour l'IHM (un `match` sans cas par défaut) |
| Nouvelle commande de l'IHM vers le worker | `handle_connection` (worker), l'envoi dans `ImportScreen` / `ResumeScreen` (§ 4) | **non** |
| Nouveau statut ou donnée de fin | `WorkerResult`, rempli en fin de `run_pipeline`, lu par `AppState::apply_worker_result` | non pour `status` (une chaîne) |
| Étape du worker avant ou après la Pass 2 | `run_pipeline` ; le cœur de l'import est dans [pass2.md](pass2.md) | non |
| Démarrage, verrou, socket | `main.rs` du worker : `LockfileGuard`, `bind_socket` ; côté IHM `spawn_worker`, `connect_with_retry` | non |
| Annulation | `CancelToken`, `spawn_signal_cancel` (worker) ; `WorkerKillHandle::terminate` (IHM) | non |
| Reprise après redémarrage de l'IHM | `find_active_socket`, `ResumeScreen`, reconnexion dans `ImportScreen` | non |

Les événements et ce qu'ils deviennent dans le protocole (leurs données : architecture.md) :

| `ProgressEvent` | Émis par | Passe par le socket ? | Rôle dans le protocole |
|---|---|---|---|
| `Pass1Log` | Pass 1 (`pass1/runner.rs`) | non : canal direct de `AnalysisScreen` | — |
| `Pass1Progress` | Pass 1 | non | — |
| `Pass1Done` | Pass 1 | non | arrête la boucle de lecture d'`AnalysisScreen` |
| `DdlStart` | `create_tables_no_constraints` | oui | — |
| `DdlProgress` | `create_tables_no_constraints` | oui | — |
| `DdlDone` | `create_tables_no_constraints` | oui | — |
| `ConstraintsStart` | `add_constraints` | oui | l'IHM en déduit que les `COPY` sont finis |
| `ConstraintsProgress` | `add_constraints` | oui | — |
| `ConstraintsDone` | `add_constraints` | oui | — |
| `Pass2Progress` | dispatcher de la Pass 2, toutes les 1 000 lignes | oui | — |
| `Pass2Flush` | flusher, après chaque `COPY` | oui | — |
| `Pass2Error` | flusher, `COPY` en échec | oui | message seulement : la fin vient du fichier résultat |
| `Pass2AnomalyUpdate` | `emit_completion_events` | oui | — |
| `Pass2Log` | Pass 2, réglages PostgreSQL (`pg_tuning.rs`) | oui | — |
| `Pass2Done` | `emit_completion_events`, en fin de `load_and_constrain` | oui | **ferme le flux** : le worker ferme chaque connexion, l'IHM cesse de lire (§ 3) |

## 2. Flux

```text
IHM : ImportScreen
  ├─ is_lockfile_free (avertissement seulement)
  ├─ new_socket_path → json2sql-<uuid>.sock ; fichier résultat = même nom en .json
  └─ spawn_worker : lance json2sql-worker (à côté de l'exécutable de l'IHM),
       écrit WorkerConfig sur stdin puis le ferme, J2S_PG_PASSWORD en variable d'environnement,
       connect_with_retry (20 essais, 100 ms) → WorkerHandle (lecture / écriture du socket)

worker : main
  ├─ lit WorkerConfig sur stdin (illisible → sortie code 2)
  ├─ LockfileGuard::try_acquire (verrou dans temp_dir : un seul worker à la fois)
  ├─ bind_socket ; spawn_signal_cancel (SIGTERM / SIGINT / SIGHUP → CancelToken)
  ├─ serve_connections (tâche à part) ──► handle_connection par client :
  │      historique complet d'ImportSummary, puis les suivants à chaque réveil ;
  │      lit les commandes du client ; ferme après Pass2Done ou à l'annulation
  ├─ run_pipeline : connexion PG (10 s) → create_tables_no_constraints → run_interruptible (voir pass2.md)
  │      les ProgressEvent passent par un canal → ImportSummary::push
  ├─ write_result (fichier .tmp puis renommage)
  └─ CancelToken annulé : fin de serve_connections, sortie

IHM : boucle d'ImportScreen
  ├─ SocketEventReader::next_event → lot appliqué toutes les 200 ms par AppState::apply_progress_event
  ├─ EOF ou erreur sans Pass2Done → reconnexion (10 fois au plus), les `seen` premiers événements rejoués sont sautés
  └─ socket parti sans Pass2Done → lit le fichier résultat → AppState::apply_worker_result

Annulation (bouton Cancel) : {"cmd":"cancel"} sur le socket ET WorkerKillHandle::terminate
  (SIGTERM, puis SIGKILL 2 min après) ; l'IHM revient tout de suite à l'écran de départ.
Reprise : main.rs de l'IHM → find_active_socket → ResumeScreen → « Reprendre » (ImportScreen sur ce socket)
  ou « Abandonner » ({"cmd":"cancel"}, puis suppression du socket et du fichier résultat).
```

## 3. Invariants et pièges

- **`Pass2Done` ferme le flux.** `ImportSummary::push` marque l'import fini à `Pass2Done`, `handle_connection` ferme
  alors la connexion, et l'IHM arrête de lire. Or `run_interruptible` remet les réglages PostgreSQL (autovacuum,
  `max_wal_size`) **après** `load_and_constrain`, donc après `Pass2Done` : ses messages `Pass2Log` n'arrivent pas à
  l'IHM, qui affiche « terminé » pendant que le worker travaille encore. Tout événement émis après `Pass2Done` est
  perdu pour l'IHM.
- **Un import en échec n'envoie pas de `Pass2Done`.** L'IHM l'apprend par le fichier résultat. D'où l'ordre dans
  `main.rs` du worker : `write_result` **avant** l'annulation qui ferme les connexions. Dans l'autre ordre, l'IHM lit un fichier
  absent et affiche « Worker disparu avant la fin de l'import ».
- **`"cancelled"` n'est jamais écrit.** Le commentaire de `WorkerResult` annonce `"success"`, `"error"` ou
  `"cancelled"`, mais `run_pipeline` range toute erreur, annulation comprise, en `"error"` (message
  « Invalid input: Pass 2 interrupted » ou « cancelled »). `AppState::apply_worker_result` ne teste que `"success"`.
  Un nouveau statut compile partout et tombe dans « erreur » côté IHM.
- **La commande d'annulation est écrite en dur, deux fois.** L'IHM n'utilise pas `WorkerCommand` : elle écrit le texte
  `{"cmd":"cancel"}` dans `ImportScreen` et dans `ResumeScreen`. `handle_connection` ignore **en silence** une ligne
  illisible ou une commande inconnue. Renommer le champ, ou changer la commande d'un seul côté, casse l'annulation par
  le socket sans erreur (le SIGTERM de `WorkerKillHandle::terminate` la rattrape, sauf en reprise : voir plus bas).
- **Socket et fichier résultat sont liés par leur nom.** Le worker reçoit les deux chemins dans `WorkerConfig`, mais
  l'IHM en déduit l'un de l'autre (`with_extension("json")`, `with_extension("sock")`) à la reprise, à la
  reconnexion et dans « Abandonner ». `find_active_socket` cherche `json2sql-*.sock` dans le dossier temporaire, la
  forme que produit `new_socket_path`. Changer l'un de ces noms casse la reprise en silence.
- **`find_active_socket` supprime les sockets qui ne répondent pas**, et ouvre une connexion sur chacun de ceux qui
  répondent : le worker y envoie son historique complet pour rien.
- **La reconnexion compte les lignes.** Chaque connexion reçoit tout l'historique depuis le début ; l'IHM saute les
  `seen` premières lignes. Cela suppose un historique d'`ImportSummary` complet et seulement ajouté : en retirer ou en
  regrouper des événements décale le compte, et l'IHM saute ou rejoue des événements sans le savoir.
- **Un événement illisible est sauté.** `SocketEventReader::next_event` rend une erreur `InvalidData`, que la boucle
  d'`ImportScreen` signale sur stderr puis ignore. Côté worker, un événement qui ne se sérialise pas n'est pas envoyé.
  Un écart de format de `ProgressEvent` entre les deux binaires perd des événements sans rien afficher.
- **Le binaire du worker peut être ancien.** L'IHM lance le `json2sql-worker` posé à côté de son propre exécutable.
  `json2sql-ui` ne dépend pas de `json2sql-worker` dans Cargo : `cargo run -p json2sql-ui` ne reconstruit pas le
  worker. Après un changement de `WorkerConfig` ou de `ProgressEvent`, un worker ancien sort en code 2 (config
  illisible) ou ses événements sont sautés. Construire avec `cargo build --workspace`.
- **Le worker a 2 s pour ouvrir son socket** (`connect_with_retry`, 20 × 100 ms). Une étape lente ajoutée dans `main.rs`
  avant `bind_socket` fait échouer le lancement.
- **Mot de passe absent = mot de passe vide.** `run_pipeline` lit `J2S_PG_PASSWORD` avec une valeur vide par défaut :
  l'erreur affichée est un refus d'authentification PostgreSQL, pas une variable manquante.
- **En reprise, l'IHM n'a pas de processus attaché.** `WorkerKillHandle` est vide : `WorkerKillHandle::terminate` ne
  fait rien, seule la commande du socket arrête le worker.
- **Un seul worker par dossier temporaire.** Le verrou (`lockfile_path`) est global ; `is_lockfile_free` n'est qu'un
  avertissement, c'est le worker qui refuse (code 1). Le verrou reste tenu pendant la remise en place des réglages,
  après `Pass2Done` : un nouvel import lancé aussitôt est refusé.
- **Le worker n'affiche rien sur stdout** (redirigé vers rien) ; ses messages `eprintln!` vont sur le stderr du
  terminal qui a lancé l'IHM. `ProgressTracker` (barres de la CLI) n'est pas utilisé par le worker.

## 4. Points d'extension

Les endroits prévus sont listés au § 1. Trois changements demandent des étapes que le compilateur ne rappelle pas.

**Nouvel événement de progression.**

- [ ] la variante dans `ProgressEvent`, et une ligne dans le tableau du § 1 (le test `guides_consistency` le vérifie) ;
- [ ] son émission ; s'il est émis par la Pass 2 ou le DDL, il passe par le socket sans autre étape ;
- [ ] son effet dans `AppState::apply_progress_event` (le compilateur le signale) ;
- [ ] s'il est émis après `Pass2Done`, il n'atteindra pas l'IHM (§ 3) ;
- [ ] s'il doit terminer le suivi comme `Pass2Done`, le traiter aussi dans `ImportSummary::push` et dans la boucle
  d'`ImportScreen`, qui testent cette variante chacun de leur côté ;
- [ ] reconstruire le worker et l'IHM ensemble.

**Nouveau réglage dans `WorkerConfig`.**

- [ ] le champ ; le compilateur signale le littéral d'`ImportScreen` et les trois de tests (`ipc.rs`, `main.rs` et
  `pipeline.rs` du worker) ;
- [ ] le lire : dans `WorkerConfig::into_pass2_config` s'il va à `Pass2Config` (voir [pass2.md](pass2.md) § 4), sinon
  dans `run_pipeline`. Rien ne signale un champ jamais lu ;
- [ ] aucun secret dans `WorkerConfig` : elle passe en clair sur stdin ; un secret passe en variable d'environnement,
  comme `J2S_PG_PASSWORD` dans `spawn_worker`.

**Nouvelle commande de l'IHM vers le worker.**

- [ ] la reconnaître dans `handle_connection` (comparaison de chaîne : une faute de frappe est ignorée en silence) ;
- [ ] l'envoyer en sérialisant `WorkerCommand` plutôt qu'en recopiant un texte JSON ;
- [ ] l'envoi n'est possible que tant que la connexion est ouverte : pas après `Pass2Done` ;
- [ ] un test dans `serve.rs`, sur le modèle de celui de l'annulation.

## 5. Vérifier

```bash
cargo test -p json2sql-worker
cargo test -p json2sql-ui worker_client
cargo test --lib ipc
cargo test --lib progress_event
cargo test --test guides_consistency
cargo build --workspace
```

`cargo test --lib --bins` à la racine ne lance ni les tests du worker ni ceux de l'IHM : le workspace n'a pas de
`default-members`, il faut `-p`. Aucun de ces tests ne demande PostgreSQL ; aucun ne lance le vrai binaire du worker
de bout en bout : le chemin complet IHM → worker → PostgreSQL se vérifie à la main.

## 6. Odeurs constatées

Vues en écrivant la fiche, pas tranchées. Voir aussi #80 (annulation lente) et #72 (test du socket instable).

1. **Protocole en chaînes** (#119). `WorkerCommand` porte une chaîne, `WorkerResult` un statut en chaîne ; la commande
   d'annulation est recopiée en texte dans deux écrans ; `"cancelled"` est annoncé et jamais écrit. Des enums sérialisés
   feraient signaler au compilateur chaque côté à modifier.
2. **Messages perdus après `Pass2Done`** (#120, non reproduit). La remise en place des réglages PostgreSQL logue après
   la fermeture du flux, et l'IHM se dit terminée avant le worker. Un événement de fin émis par le worker après
   `run_interruptible` réglerait les deux.
3. **Deux usages de `Notify` à vérifier** (#121, non reproduit). `CancelToken::cancelled` lit l'état puis se met en
   attente : une annulation qui tombe entre les deux ne réveille rien (`notify_waiters` ne garde pas de trace), et la
   Pass 2 ne serait arrêtée que par le SIGKILL de secours. `CancellationToken` de tokio-util, déjà utilisé par la Pass
   2, fait la même chose sans ce trou. `ImportSummary::push` réveille un seul client (`notify_one`) : avec deux
   connexions, l'autre attend l'événement suivant.
4. **Fichiers temporaires** (#122). Le chemin du résultat est déduit de celui du socket en trois endroits de l'IHM ; le
   socket et le fichier résultat ne sont jamais supprimés après un import normal, seulement par « Abandonner » ou au
   démarrage suivant de l'IHM (sockets seulement).
5. **Tests recopiés** (#123). La `WorkerConfig` de test existe en trois copies (`ipc.rs`, `main.rs`, `pipeline.rs`), et
   le test de `WorkerConfig::into_pass2_config` en deux (`ipc.rs`, `main.rs`).
