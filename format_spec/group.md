# Group

A group consists of [metadata](./metadata.md) anda file containing group members

```
my_group                       # Group folder
    |_ __tiledb_group.tdb      # Empty group file
    |_ __group                 # Group folder
        |_ <timestamped_name>  # Timestamped group file detailing members
```

## Group File


| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Version | `uint32_t` | Format version number of the group |
| Number of Group Member | `uint32_t` | The number of group members. |
| Group Member 1 | [Group Member](#group Member) | First group member |
| … | … | … |
| Group Member N | [Group Member](#group Member) | Nth group member |


## Group Member

The group member is the content inside a [group](./group.md)

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Version | `uint32_t` | Format version number of the group member |
| URI length | `uint32_t` | Number of characters in uri |
| URI | `char[]` | URI character array |
| Object type | `uint8_t` | Object type of the member |
