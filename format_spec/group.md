# Group

The current group format version (`2`) is a folder called `__group` located here:

```
my_group                       # Group folder
    |  ...
    |_ __group                 # Group folder
        |_ <timestamped_name>  # Timestamped group file detailing members
    |_ ...
```

The group folder can contain:

* Any number of group files with name [`<timestamped_name>`](./timestamped_name.md).

## Group File


| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Version | `uint32_t` | Format version number of the group |
| Number of Group Member | `uint32_t` | The number of group members. |
| Group Member 1 | [Group Member](##Group Member) | First group member |
| … | … | … |
| Group Member N | [Group Member](##Group Member) | Nth group member |


## Group Member

The group member is the content inside a [group](./group.md).

The current group member format version is `2`.

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Version | `uint32_t` | Format version number of the group member |
| Object type | `uint8_t` | Object type of the member |
| Relative | `uint8_t` | Is the URI relative to the group |
| URI length | `uint64_t` | Number of characters in URI |
| URI | `uint8_t[]` | URI character array |
| Name set | `uint8_t` | Is the name set |
| Name length | `uint64_t` | Number of characters in name |
| Name | `uint8_t[]` | Name character array |
| Deleted | `uint8_t` | Is the member deleted |
