!
<br> Все ссылания на переменные идут для `.env`
<br>В котором находятся все основные переменные для корректировки.
<br>Создайте или используйте базовый с редакцией **логин/пароль**.
<br>!
------------------------------
Необходимые предустановки
------------------------------
- [Docker](https://www.docker.com/)
- [WSL2](https://learn.microsoft.com/ru-ru/windows/wsl/install) <- используется самим Docker (если используете без него, должно работать и так)<br>Используется для создания ключа, поэтому если ключ будет создаваться по пункту `1` необходимо к использованию<br>(либо используйте другой способ создания ключа)
------------------------------
Последовательность запуска:
------------------------------
Все приведённые команды с учётом запуска из рабочей директории приложения.
1. **Создание MongoDB `keyFile` в директории `docker_mongo_db`**:
    ```commandline
    wsl -u root
    apt-get install openssl
    openssl rand -base64 756 > ./docker_mongo_db/mongo-key.txt
    ```
   <u>756 <- устанавливает количество байтов для генерации.
   <br>Большее значение может обеспечить более высокую степень случайности, но для MongoDB важно, чтобы размер ключа был не менее **1024** символов после кодирования в base64.</u>
2. **Для использования аутентификации с использованием `JWT`**  
   Необходимо использовать публичный ключ сертификата использованного для создания `JWT` в сервисе авторизации.
   Поместите его в директорию `./auth/` со стандартным именем =`public_key.pem`.
   Либо измените переменную `JWT_PUBLIC_KEY_NAME` на желаемое имя для использования.
   (Если нет необходимости использовать валидацию JWTN -> установите `JWT_VALIDATION_TOKEN_REQ=False`)
3. **Запуск Docker:**
   1. Из рабочей директории приложения с `docker-compose.yml` 
    ```commandline
    docker-compose up --no-start --build -d
    ``` 
   2. Запустите контейнер MongoDB
   ```commandline
   docker start mongo_grid_api
   ```
   3. Инициализируйте `replica-set`
    ```commandline
    docker exec -it mongo_grid_api bash -c 'replica_init.sh'
    ```
   4. Создайте пользователя для сервиса 
    ```commandline
    docker exec -it mongo_grid_api bash -c 'create_api_user.sh'
    ```
   5. Запустите контейнер сервиса
   ```commandline
   docker start grid_api_app
   ```
    `mongo_grid_api` & `grid_api_app`  <- стандартное название контейнера, замените на используемый вами.
    <br>Также можно запустить все команды и через exec в Desktop версии.
4. **Проверьте рабочий статус сервиса перейдя `localhost:8000`**
   <br>(Измените домейн + порт в соответствии с вашими данными)
------------------------------
Описание переменных `.env`:
------------------------------
- `jwt_algorithm` <- алгоритм использумый для создания JWT
- `jwt_issuer` <- создатель(издатель) токена, хранящийся в нём для верификации
- `JWT_VALIDATION_TOKEN_REQ` <- True = эндпойнты сервиса при запросах будут требовать JWT для верификации | False = эндпойнты не будет требовать верификации
- `JWT_PUBLIC_KEY_NAME` <- имя файла для поиска в директории `app/auth/` (`app` основная директория сервиса)
- `MONGO_SERVER` <- адрес подключения к MongoDB
- `MONGO_SERVER_OUTSIDE_PORT` <- открываемый внешний порт для контейнера базы данных
- `MONGO_SERVER_INSIDE_PORT` <- открываемый внутренний порт для контейнера базы данных
- `MONGO_REPLICA_NAME` <- название создаваемого и используемого в дальнейшем `replica-set` (используется при подключении)
- `MONGO_CONTAINER_NAME` <- название создаваемого контейнера
- `MONGO_ADMIN_LOGIN` <- логин для аутентификации первичного аккаунта администратора базы данных (имеет все права)
- `MONGO_ADMIN_PWD` <- пароль для аутентификации первичного аккаунта администратора базы данных
- `API_MONGO_LOGIN` <- логин для аутентификации аккаунта используемого сервисом (имеет доступ только к выделенной базе данных)
- `API_MONGO_PWD` <- пароль для аутентификации аккаунта используемого сервисом
- `API_MONGO_DB_NAME` <- название базы данных, используемой сервисом
- `API_MONGO_AUTH_DATABASE` <- база данных используемая для авторизации аккаунта сервиса
- `API_CONTAINER_NAME` <- название контейнера сервиса
- `API_OUTSIDE_PORT` <- открываемый внешний порт для контейнера сервиса
- `API_INSIDE_PORT` <- открываемый внутренний порт для контейнера сервиса
- `CREATE_PMK_PRESETS` <- создаём базовый Приямок + Челнок для пресета == ПМК
- `PMK_GRID_NAME` <- имя Приямка которое будет использоваться при создании **(не желательно менять)**
- `PMK_PLATFORM_NAME` <- имя Челнока которое будет использоваться при создании **(не желательно менять)**
