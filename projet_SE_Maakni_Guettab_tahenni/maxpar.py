import os
import threading
import time
from queue import Queue
from typing import Dict

import matplotlib.pyplot as plt
import networkx as nx


class TaskNotFoundError(Exception):
    # Cette classe définit une erreur spécifique qui sera levée lorsque la tâche recherchée n'est pas présente dans la
    # liste des tâches
    def __init__(self, task_name):
        super().__init__(f'Task "{task_name}" not found in the list of tasks')


class DuplicateTaskError(Exception):
    # Cette classe définit une erreur spécifique qui sera levée lorsque plusieurs tâches portent le même nom
    def __init__(self, task_name):
        super().__init__(f'Duplicate task found: "{task_name}"')


class NoTaskPrecedenceError(Exception):
    # Cette classe définit une erreur spécifique qui sera levée lorsqu'une tâche n'a pas de précédence
    def __init__(self, task_name):
        super().__init__(f'The task "{task_name}" has no precedence')


class InvalidPrecedenceDictError(Exception):
    # Cette classe définit une erreur spécifique qui sera levée lorsque le dictionnaire de précédence est invalide
    pass


class Task:
    # Cette classe définit une tâche qui doit être exécutée par le programme
    def __init__(self, name: str, reads: list, writes: list, run=None):
        # Initialise les attributs de la tâche
        self.name = name  # nom de la tâche
        self.reads = reads  # liste des variables globale que la tâche lit
        self.writes = writes  # liste des variables globale que la tâche écrit
        self.run = run  # fonction qui sera exécutée pour la tâche (facultatif)

    def __repr__(self):
        # Retourne une représentation en chaîne de la tâche
        return f'<maxpar.Task "{self.name}">'


class TaskSystem:
    LOGING = True

    # Définition d'une variable de classe LOGING à True
    # La variable LOGGING dans la classe TaskSystem est utilisée pour indiquer si les informations relatives au
    # déroulement du système de tâches doivent être affichées ou non. Si la valeur de cette variable est True, les
    # informations de journalisation seront affichées, sinon elles ne le seront pas.

    def __init__(self, tasks: list[Task], precedence: Dict[str, list[str]]):
        self.tasks: dict[str, Task] = {}  # Création d'un dictionnaire pour stocker les tâches
        self.precedence = precedence  # Stockage des précédences
        # Vérification des erreurs et déclenchement des exceptions personnalisées
        # - Noms de tâches en double
        # - Précédence de tâche si elle existe
        # - Vérification du graph de précédences
        task_names = set()
        for task in tasks:
            if precedence.get(task.name) is None:
                raise NoTaskPrecedenceError(task.name)  # Lever une exception si la précédence de la tâche n'existe pas
            if task.name in task_names:
                raise DuplicateTaskError(task.name)  # Lever une exception si un nom de tâche est en double
            task_names.add(task.name)
            self.tasks[task.name] = task  # Stockage de la tâche dans le dictionnaire self.tasks

        for task_name in precedence:
            if self.tasks.get(task_name) is None:
                raise TaskNotFoundError(task_name)  # Lever une exception si une tâche n'est pas trouvée

        self.__validate_precedence_dict(precedence)  # Vérification du graph de précédences

    def __validate_precedence_dict(self, precedence_dict):
        # Création d'un dictionnaire pour suivre les dépendances
        dependencies = {}
        for task, dependent_tasks in precedence_dict.items():
            # Vérification si la tâche dépend d'elle-même
            if task in dependent_tasks:
                raise InvalidPrecedenceDictError(f"Task {task} cannot depend on itself")
            # Ajout de la tâche au dictionnaire des dépendances
            if task not in dependencies:
                dependencies[task] = set()
            # Ajout de tâches dépendantes au dictionnaire des dépendances
            for dependent_task in dependent_tasks:
                if dependent_task not in dependencies:
                    dependencies[dependent_task] = set()
                dependencies[dependent_task].add(task)

        # Vérification des cycles dans le graph des dépendances en utilisant la recherche en profondeur
        visited = set()
        stack = set()
        for task in dependencies:
            if task not in visited:
                if self.__dfs_cycle_check(task, visited, stack, dependencies):
                    raise InvalidPrecedenceDictError("Precedence dictionary contains a cycle")

    def __dfs_cycle_check(self, task_name, visited, stack, dependencies):
        """
        Cette méthode effectue une recherche en profondeur sur le graphe de dépendances pour détecter les cycles.
        Elle prend quatre paramètres :

        :param task_name: le nom de la tâche en cours de visite.
        :param visited: un ensemble de toutes les tâches visitées jusqu'à présent.
        :param stack: un ensemble de toutes les tâches actuellement empilées en cours d'exploration.
        :param dependencies: un dictionnaire qui associe chaque tâche à son ensemble de tâches dépendantes.


        La méthode commence par ajouter la tâche actuelle aux ensembles visited et stack.
        Elle parcourt ensuite chaque tâche dépendante de la tâche actuelle.
        Si une tâche dépendante n'a pas été visitée, la méthode s'appelle elle-même de manière récursive sur
        la tâche dépendante. Si l'appel récursif renvoie True, cela signifie qu'un cycle a été détecté, donc la
        méthode renvoie également True. Si une tâche dépendante est déjà dans l'ensemble stack, cela signifie
        qu'un cycle a été détecté, ainsi la méthode renvoie True.

        Après avoir exploré toutes les tâches dépendantes de la tâche actuelle, la méthode supprime la tâche
        actuelle de l'ensemble stack et renvoie False, indiquant qu'aucun cycle n'a été détecté pour la tâche actuelle.
        """
        visited.add(task_name)
        stack.add(task_name)
        for dependent_task in dependencies[task_name]:
            if dependent_task not in visited:
                if self.__dfs_cycle_check(dependent_task, visited, stack, dependencies):
                    return True
            elif dependent_task in stack:
                return True
        stack.remove(task_name)
        return False

    def get_dependencies(self, task_name: str) -> list[str]:
        """
        Cette méthode sert à récupérer toutes les dépendances d'une tâche donnée, c'est-à-dire toutes les tâches qui
        doivent être exécutées avant que la tâche donnée puisse être exécutée. Les commentaires expliquent comment la
        méthode fonctionne : elle vérifie d'abord que la tâche donnée existe dans la liste des tâches, puis elle
        initialise une liste vide dependencies et un ensemble vide visited pour stocker les dépendances et les tâches
        déjà visitées, respectivement. Ensuite, elle appelle une méthode récursive __r__get_dependencies() pour trouver
        toutes les dépendances de la tâche donnée. Enfin, elle retourne la liste de dépendances trouvées, en retirant la
        tâche donnée elle-même. Si la tâche donnée n'existe pas, elle lève une exception TaskNotFoundError.
        :param task_name: le nom de la tache
        """
        # Vérifie si la tâche existe dans la liste des tâches
        if self.precedence.get(task_name) is not None:
            dependencies = []
            visited = set()
            # Appelle la méthode récursive pour trouver toutes les tâches dont dépend la tâche donnée
            self.__r__get_dependencies(task_name, dependencies, visited)
            # Retourne la liste de dépendances, en retirant la tâche donnée elle-même
            return dependencies[:-1]
        else:
            # Si la tâche n'existe pas, lève une exception
            raise TaskNotFoundError(task_name)

    def __r__get_dependencies(self, task_name: str, dependencies: list[str], visited: set[str]):
        """
        Il s'agit d'une méthode récursive privée de la classe TaskSystem.
        Elle est utilisée pour obtenir la liste des dépendances d'une tâche donnée.
        La méthode commence par vérifier si la tâche a déjà été visitée.
        Si oui, cela signifie que nous avons déjà ajouté ses dépendances à la liste dependencies et nous pouvons
        simplement retourner. Sinon, nous allons chercher les dépendances de la tâche en utilisant le dictionnaire
        precedence qui a été passé à l'initialisation de l'objet TaskSystem.
        Si la tâche n'a pas de dépendances, nous l'ajouterons uniquement à la liste dependencies et la marquons comme
        visitée.
        Si elle a des dépendances, nous appelons récursivement __r__get_dependencies() pour chacune de ses
        dépendances. Ensuite, nous ajoutons la tâche elle-même à la liste dependencies et la marquons comme visitée.

        À la fin de l'appel de la méthode pour la tâche initiale, la liste dependencies contient les noms de toutes les
        tâches dont la tâche initiale dépend directement ou indirectement.
        Cette liste est ensuite retournée par la méthode get_dependencies()

        :param task_name: le nom de la tâche pour laquelle on veut obtenir les dépendances
        :param dependencies: une liste qui va contenir les noms des tâches dépendantes
        :param visited: un ensemble pour garder une trace des tâches déjà visitées
        :return:
        """
        # Vérifie si la tâche courante n'a pas été visitée précédemment pour éviter les boucles infinies
        if task_name not in visited:
            # Vérifie si la tâche a des dépendances
            sub_dependencies = self.precedence.get(task_name)
            if len(sub_dependencies) == 0:
                # Si la tâche n'a pas de dépendance, ajoute-la à la liste de dépendances
                dependencies.append(task_name)
                visited.add(task_name)
            else:
                # Si la tâche a des dépendances, parcourt chaque dépendance et ses sous-dépendances
                for sub_task_name in sub_dependencies:
                    self.__r__get_dependencies(sub_task_name, dependencies, visited)

                # Ajoute la tâche courante à la liste de dépendances après avoir ajouté toutes les dépendances
                dependencies.append(task_name)
                visited.add(task_name)

    def run_seq(self):
        """
        Cette methode exécute toutes les tâches dans une séquence déterminée par l'ordre des dépendances.
        Elle commence par obtenir toutes les tâches à exécuter en appelant la méthode privée __r___get_tasks_queue()
        qui utilise une approche de parcours en profondeur pour ajouter toutes les tâches dépendantes d'une tâche à une
        file d'attente.

        Ensuite, la méthode itère sur la file d'attente et exécute chaque tâche en appelant sa méthode run().
        Si la variable de classe LOGING est vraie, elle affiche un message indiquant quelle tâche est en cours
        d'exécution et sur quel processus.

        En fin de compte, toutes les tâches sont exécutées dans l'ordre correct déterminé par les dépendances,
        garantissant ainsi la cohérence des données produites.
        """

        # Crée un ensemble de tâches déjà visitées et une file d'attente de tâches à exécuter
        visited = set()
        sequence: Queue[Task] = Queue()

        # Parcourt toutes les tâches pour les ajouter à la file d'attente dans l'ordre correct en respectant les dépendances
        for task_name in self.tasks:
            self.__r___get_tasks_queue(task_name, sequence, visited)

        # Exécute toutes les tâches dans la file d'attente
        while not sequence.empty():
            task = sequence.get()
            if self.LOGING:
                print(f'[maxpar] Running {task.name} on process {os.getpid():6} ...')
            task.run()

    def __r___get_tasks_queue(self, task_name: str, sequence: Queue[Task], visited: set):
        """
        Cette méthode utilise une approche récursive pour construire une séquence de tâches qui doivent être exécutées
        dans un ordre spécifique pour satisfaire toutes les dépendances.

        Elle commence par vérifier si la tâche a déjà été visitée, si c'est le cas, elle retourne immédiatement.
        Sinon, elle obtient les dépendances de la tâche en appelant la méthode get_dependencies définie précédemment.

        Ensuite, elle vérifie si toutes les dépendances ont déjà été visitées.
        Si ce n'est pas le cas, elle appelle cette méthode pour chaque dépendance manquante.

        Cela garantit que toutes les dépendances des tâches sont visitées avant d'ajouter la tâche elle-même à la
        file d'attente.

        Enfin, elle ajoute la tâche visitée à la file d'attente et l'ajoute également à l'ensemble des tâches visitées.
        :param task_name: le nom d'une tâche
        :param sequence: une file d'attente vide qui sera remplie avec la séquence des tâches
        :param visited: un ensemble de tâches déjà visitées

        """
        # Vérifie si la tâche a déjà été visitée et quitte la méthode si c'est le cas
        if task_name in visited:
            return
        # Obtient les dépendances de la tâche
        dependencies = self.get_dependencies(task_name)
        # Vérifie si toutes les dépendances ont déjà été visitées
        if not (len(dependencies) == 0 or all(dependency in visited for dependency in dependencies)):
            # Si ce n'est pas le cas, on appelle récursivement cette méthode pour chaque dépendance
            for dependency in dependencies:
                self.__r___get_tasks_queue(dependency, sequence, visited)

        # Obtient la tâche correspondant au nom de la tâche et l'ajoute à la file d'attente
        task = self.tasks.get(task_name)
        sequence.put(task)
        # Marque la tâche comme visitée
        visited.add(task_name)

    def run(self):
        """
        La méthode run exécute toutes les tâches dans l'ordre en utilisant un maximum de parallélisme possible en
        vérifiant si une tâche peut être exécutée simultanément avec d'autres tâches en cours d'exécution.
        Pour cela, elle utilise des threads et des queues pour gérer l'ordre d'exécution des tâches.
        La méthode vérifie également si une tâche peut être exécutée en se basant sur ses dépendances et ses lectures
        et écritures pour éviter les conflits d'accès aux données partagées entre les threads.
        """
        visited = set()
        queue: Queue[Task] = Queue()
        # Récupérer toutes les tâches dans l'ordre
        for task_name in self.tasks:
            self.__r___get_tasks_queue(task_name, queue, visited)

        # Exécuter toutes les tâches dans la queue en utilisant une parallélisation maximale

        # Initialiser une liste pour suivre les processus en cours d'exécution
        processes = {}
        executed = set()
        # Boucler sur les tâches dans la queue
        while not queue.empty():
            # Obtenir la prochaine tâche de la queue
            task = queue.get()

            # Vérifier si la tâche peut être exécutée simultanément avec l'un des processus en cours d'exécution
            can_run_simultaneously = True
            for p in processes:
                if set(task.reads) & set(processes[p].writes) or set(task.writes) & set(processes[p].reads):
                    # La tâche partage des lectures ou des écritures avec un processus en cours d'exécution, ne peut pas être exécutée simultanément
                    can_run_simultaneously = False
                    break

            if not set(self.get_dependencies(task.name)).issubset(executed):
                can_run_simultaneously = False

            if can_run_simultaneously:
                # Créer un nouveau processus pour exécuter la tâche
                p = threading.Thread(target=task.run)
                p.start()
                if self.LOGING:
                    print(f'[maxpar] Running {task.name} on process {p.native_id:6} ...')
                processes[p] = task
            else:
                # Remettre la tâche à la fin de la queue
                queue.put(task)

            # Attendre que les processus terminés soient terminés et les supprimer de la liste
            for p in processes.copy():
                if not p.is_alive():
                    task_done = processes.pop(p)
                    executed.add(task_done.name)
        # Attendre que tous les processus restants soient terminés
        for p in processes:
            p.join()

    def par_cost(self):
        """
        Cette methode affiche le temps d'exécution des méthodes run_seq() et run() de la classe TaskSystem. Elle mesure
        le temps d'exécution de chaque méthode en appelant la méthode time.time() avant et après chaque méthode, puis en
        calculant la différence entre les temps de fin et de début. Les temps d'exécution sont affichés à la fin de
        chaque méthode. Cela permet de mesurer la différence de temps d'exécution entre les deux méthodes, qui
        correspondent à deux approches différentes pour exécuter les tâches du système de tâches.
        """
        print("[maxpar] Starting TaskSystem.run_seq()")
        start_time = time.time()
        self.run_seq()
        end_time = time.time()
        print(f"Execution time: {round(end_time - start_time, 4)} seconds\n")

        print("[maxpar] Starting TaskSystem.run()")
        start_time = time.time()
        self.run()
        end_time = time.time()
        print(f"Execution time: {round(end_time - start_time, 4)} seconds")

    def visualize(self):
        """
        Visualise l'arbre d'exécution des tâches du système
        """
        # Créer un graphe dirigé
        graph = nx.DiGraph()

        # Ajouter les nœuds au graphe
        for task in self.precedence.keys():
            graph.add_node(task)

        # Ajouter les arêtes au graphe
        for task, dependencies in self.precedence.items():
            for dependency in dependencies:
                graph.add_edge(dependency, task)

        # La variable pos est définie en utilisant la fonction nx.kamada_kawai_layout qui calcule la disposition optimale des nœuds en fonction de leurs interconnexions
        # Cette fonction a besoin de la librairie scipy
        pos = nx.kamada_kawai_layout(graph)

        # Dessiner le graphe
        nx.draw(
            graph,
            pos,
            with_labels=True,
            node_size=900,
            node_color='lightblue',
            font_size=12,
            font_weight='bold',
            width=2
        )
        plt.show()

    def set_logging(self, state: bool):
        self.LOGING = state
