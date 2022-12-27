migrate((db) => {
  const collection = new Collection({
    "id": "n0vrc5a0fv73za0",
    "created": "2022-12-20 04:22:48.242Z",
    "updated": "2022-12-20 04:22:48.242Z",
    "name": "pupils",
    "type": "base",
    "system": false,
    "schema": [
      {
        "system": false,
        "id": "2s5wigj5",
        "name": "pupilName",
        "type": "text",
        "required": false,
        "unique": false,
        "options": {
          "min": null,
          "max": null,
          "pattern": ""
        }
      }
    ],
    "listRule": null,
    "viewRule": null,
    "createRule": null,
    "updateRule": null,
    "deleteRule": null,
    "options": {}
  });

  return Dao(db).saveCollection(collection);
}, (db) => {
  const dao = new Dao(db);
  const collection = dao.findCollectionByNameOrId("n0vrc5a0fv73za0");

  return dao.deleteCollection(collection);
})
