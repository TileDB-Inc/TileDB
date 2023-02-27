# Group

A group consists of [metadata](./metadata.md) and a file containing group members

```
my_group                       # Group folder
    |_ __tiledb_group.tdb      # Empty group file
    |_ __group                 # Group folder
        |_ <timestamped_name>  # Timestamped group file detailing members
    |_ __meta                  # group metadata folder
```

## Group File


| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Version | `uint32_t` | Format version number of the group |
| Number of Group Member | `uint32_t` | The number of group members. |
| Group Member 1 | [Group Member](##Group Member) | First group member |
| … | … | … |
| Group Member N | [Group Member](##Group Member) | Nth group member |


## Group Member

The group member is the content inside a [group](./group.md)

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Version | `uint32_t` | Format version number of the group member |
| Object type | `uint8_t` | Object type of the member |
| Relative | `uint8_t` | Is the URI relative to the group |
| URI length | `uint32_t` | Number of characters in URI |
| URI | `uint8_t[]` | URI character array |
| Deleted | `uint8_t` | Is the member deleted |
