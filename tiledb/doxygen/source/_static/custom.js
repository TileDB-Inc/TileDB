$(function() {
    /**
     * Override the default content-tabs selection logic.
     */
    $('.contenttab-selector li').off('click').click(function(evt) {
        evt.preventDefault();

        /**
         * For all tab blocks in the document, select the tabs with the same
         * class as this one. If a tab block doesn't have a matching tab,
         * do nothing (don't change its selection).
         */

        var sel_class = $(this).attr('class');

        $('div.content-tabs').each(function() {
            var selectors = $(this).find('ul.contenttab-selector');
            var has_sel_class = $(selectors).find('.' + sel_class).length > 0;
            // Update the visibility and selected classes for the tabs in the
            // current content-tab group.
            if (has_sel_class) {
                $(this).find('div.contenttab').hide();
                $(this).find('div#' + sel_class).show();
                $(selectors).find('li').removeClass('selected');
                $(selectors).find('li.' + sel_class).addClass('selected');
            }
        });
    });
});