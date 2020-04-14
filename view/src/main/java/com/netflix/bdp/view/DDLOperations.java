package com.netflix.bdp.view;

/**
 * View operations that lead to a new version of view getting created.
 * <p>
 * A version can return the operation that resulted in creating that version of the view.
 * Users can inspect the operation to get more information in case a rollback is desired.
 */
public class DDLOperations {
    private DDLOperations() {
    }

    /**
     * The view was created.
     */
    public static final String CREATE = "create";

    /**
     * View definition was replaced. This operation covers actions such as associating a different
     * schema with the view, adding a column comment etc.
     */
    public static final String REPLACE = "replace";

    /**
     * View column comments were altered.
     */
    public static final String ALTER_COMMENT = "alter-comment";
}
